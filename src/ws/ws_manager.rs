use crate::{
    prelude::*,
    ws::message_types::{AllMids, Bbo, Candle, L2Book, OrderUpdates, Trades, User},
    ActiveAssetCtx, ActiveAssetData, Error, Notification, UserFills, UserFundings,
    UserNonFundingLedgerUpdates, WebData2,
};
use futures_util::{stream::SplitSink, SinkExt, StreamExt};
use log::{debug, error, info, trace, warn};
use serde::{Deserialize, Serialize};
use std::{
    borrow::BorrowMut,
    collections::HashMap,
    ops::DerefMut,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    net::TcpStream,
    spawn,
    sync::{mpsc::UnboundedSender, Mutex},
    time,
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{self, protocol},
    MaybeTlsStream, WebSocketStream,
};

use super::ActiveSpotAssetCtx;
use crate::Error::Websocket;
use ethers::types::H160;
use futures_util::stream::SplitStream;

#[derive(Debug)]
struct SubscriptionData {
    sending_channel: UnboundedSender<Message>,
    subscription_id: u32,
    id: String,
}
#[derive(Debug)]
pub(crate) struct WsManager {
    stop_flag: Arc<AtomicBool>,
    writer: Arc<Mutex<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, protocol::Message>>>,
    subscriptions: Arc<Mutex<HashMap<String, Vec<SubscriptionData>>>>,
    subscription_id: u32,
    subscription_identifiers: HashMap<u32, String>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "camelCase")]
pub enum Subscription {
    AllMids,
    Notification { user: H160 },
    WebData2 { user: H160 },
    Candle { coin: String, interval: String },
    L2Book { coin: String },
    Trades { coin: String },
    OrderUpdates { user: H160 },
    UserEvents { user: H160 },
    UserFills { user: H160 },
    UserFundings { user: H160 },
    UserNonFundingLedgerUpdates { user: H160 },
    ActiveAssetCtx { coin: String },
    ActiveAssetData { user: H160, coin: String },
    Bbo { coin: String },
}

#[derive(Deserialize, Clone, Debug)]
#[serde(tag = "channel")]
#[serde(rename_all = "camelCase")]
pub enum Message {
    NoData,
    HyperliquidError(String),
    AllMids(AllMids),
    Trades(Trades),
    L2Book(L2Book),
    User(User),
    UserFills(UserFills),
    Candle(Candle),
    SubscriptionResponse,
    OrderUpdates(OrderUpdates),
    UserFundings(UserFundings),
    UserNonFundingLedgerUpdates(UserNonFundingLedgerUpdates),
    Notification(Notification),
    WebData2(WebData2),
    ActiveAssetCtx(ActiveAssetCtx),
    ActiveAssetData(ActiveAssetData),
    ActiveSpotAssetCtx(ActiveSpotAssetCtx),
    Bbo(Bbo),
    Pong,
}

#[derive(Serialize)]
pub(crate) struct SubscriptionSendData<'a> {
    method: &'static str,
    subscription: &'a serde_json::Value,
}

#[derive(Serialize)]
pub(crate) struct Ping {
    method: &'static str,
}

impl WsManager {
    const SEND_PING_INTERVAL: u64 = 30;

    pub(crate) async fn new(url: String, reconnect: bool) -> Result<WsManager> {
        let stop_flag = Arc::new(AtomicBool::new(false));

        let (writer, reader) = Self::connect(&url).await?.split();
        let writer = Arc::new(Mutex::new(writer));

        let subscriptions_map: HashMap<String, Vec<SubscriptionData>> = HashMap::new();
        let subscriptions = Arc::new(Mutex::new(subscriptions_map));
        let subscriptions_copy = Arc::clone(&subscriptions);

        {
            let writer = writer.clone();
            let stop_flag = Arc::clone(&stop_flag);
            let url = url.clone();

            let combined_task = async move {
                let mut reader = reader;
                let mut ping_interval = tokio::time::interval(Duration::from_secs(Self::SEND_PING_INTERVAL));
                ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

                // 用于跟踪最后一次收到 pong 的时间
                let mut last_pong_time = tokio::time::Instant::now();
                let pong_timeout = Duration::from_secs(Self::SEND_PING_INTERVAL * 3); // 超时时间设为 ping 间隔的两倍

                loop {
                    if stop_flag.load(Ordering::Relaxed) {
                        break;
                    }

                    tokio::select! {
                        // 处理 WebSocket 消息
                        msg = reader.next() => {
                            match msg {
                                Some(Ok(data)) => {
                                    // 检查是否是 pong 消息
                                    if let Ok(text) = data.to_text() {
                                        if text.contains("pong") {
                                            last_pong_time = tokio::time::Instant::now();
                                            trace!("Received pong response");
                                            continue;
                                        }
                                    }

                                    // 处理其他消息
                                    if let Err(err) = WsManager::parse_and_send_data(Ok(data), &subscriptions_copy).await {
                                        error!("Error processing data received by WsManager: {err}");
                                    }
                                }
                                Some(Err(err)) => {
                                    error!("WebSocket error: {err}");
                                    // 触发重连逻辑
                                    if let Err(err) = Self::handle_disconnection_and_reconnect(
                                        &url,
                                        reconnect,
                                        &writer,
                                        &mut reader,
                                        &subscriptions_copy,
                                        &mut last_pong_time,
                                    ).await {
                                        error!("Failed to reconnect: {err}");
                                        break;
                                    }
                                }
                                None => {
                                    warn!("WsManager disconnected");
                                    // 触发重连逻辑
                                    if let Err(err) = Self::handle_disconnection_and_reconnect(
                                        &url,
                                        reconnect,
                                        &writer,
                                        &mut reader,
                                        &subscriptions_copy,
                                        &mut last_pong_time,
                                    ).await {
                                        error!("Failed to reconnect: {err}");
                                        break;
                                    }
                                }
                            }
                        }

                        // 定时发送 ping
                        _ = ping_interval.tick() => {
                            // 检查 pong 超时
                            if tokio::time::Instant::now().duration_since(last_pong_time) > pong_timeout {
                                warn!("Pong timeout detected, connection might be dead");
                                // 触发重连逻辑
                                if let Err(err) = Self::handle_disconnection_and_reconnect(
                                    &url,
                                    reconnect,
                                    &writer,
                                    &mut reader,
                                    &subscriptions_copy,
                                    &mut last_pong_time,
                                ).await {
                                    error!("Failed to reconnect after pong timeout: {err}");
                                    break;
                                }
                                continue;
                            }

                            // 发送 ping
                            match serde_json::to_string(&Ping { method: "ping" }) {
                                Ok(payload) => {
                                    let mut writer_guard = writer.lock().await;
                                    if let Err(err) = writer_guard.send(protocol::Message::Text(payload)).await {
                                        error!("Error sending ping: {err}");
                                        // 发送失败，触发重连
                                        drop(writer_guard); // 释放锁
                                        if let Err(err) = Self::handle_disconnection_and_reconnect(
                                            &url,
                                            reconnect,
                                            &writer,
                                            &mut reader,
                                            &subscriptions_copy,
                                            &mut last_pong_time,
                                        ).await {
                                            error!("Failed to reconnect after ping error: {err}");
                                            break;
                                        }
                                    }
                                }
                                Err(err) => error!("Error serializing ping message: {err}"),
                            }
                        }
                    }
                }

                warn!("WebSocket manager task stopped");
            };

            spawn(combined_task);
        }

        Ok(WsManager {
            stop_flag,
            writer,
            subscriptions,
            subscription_id: 0,
            subscription_identifiers: HashMap::new(),
        })
    }

    async fn handle_disconnection_and_reconnect(
        url: &str,
        reconnect: bool,
        writer: &Arc<
            Mutex<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, protocol::Message>>,
        >,
        reader: &mut SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
        subscriptions: &Arc<Mutex<HashMap<String, Vec<SubscriptionData>>>>,
        last_pong_time: &mut tokio::time::Instant,
    ) -> Result<()> {
        // 通知所有订阅者连接已断开
        if let Err(err) = WsManager::send_to_all_subscriptions(subscriptions, Message::NoData).await
        {
            warn!("Error sending disconnection notification: {err}");
        }

        if !reconnect {
            error!("WsManager reconnection disabled. Will not reconnect.");
            return Err(Websocket("Reconnection disabled".to_string()));
        }

        // 重连循环
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            info!("WsManager attempting to reconnect");

            match Self::connect(url).await {
                Ok(ws) => {
                    let (new_writer, new_reader) = ws.split();
                    *reader = new_reader;

                    let mut writer_guard = writer.lock().await;
                    *writer_guard = new_writer;

                    // 重新订阅
                    for (identifier, v) in subscriptions.lock().await.iter() {
                        if identifier.eq("userEvents") || identifier.eq("orderUpdates") {
                            for subscription_data in v {
                                if let Err(err) =
                                    Self::subscribe(writer_guard.deref_mut(), &subscription_data.id)
                                        .await
                                {
                                    error!("Could not resubscribe {identifier}: {err}");
                                }
                            }
                        } else if let Err(err) =
                            Self::subscribe(writer_guard.deref_mut(), identifier).await
                        {
                            error!("Could not resubscribe {identifier}: {err}");
                        }
                    }

                    // 重置 pong 时间
                    *last_pong_time = tokio::time::Instant::now();

                    info!("WsManager reconnect finished");
                    return Ok(());
                }
                Err(err) => {
                    error!("Could not reconnect to websocket: {err}");
                    // 继续重试
                }
            }
        }
    }

    async fn connect(url: &str) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>> {
        Ok(connect_async(url)
            .await
            .map_err(|e| Error::Websocket(e.to_string()))?
            .0)
    }

    fn get_identifier(message: &Message) -> Result<String> {
        match message {
            Message::AllMids(_) => serde_json::to_string(&Subscription::AllMids)
                .map_err(|e| Error::JsonParse(e.to_string())),
            Message::User(_) => Ok("userEvents".to_string()),
            Message::UserFills(fills) => serde_json::to_string(&Subscription::UserFills {
                user: fills.data.user,
            })
            .map_err(|e| Error::JsonParse(e.to_string())),
            Message::Trades(trades) => {
                if trades.data.is_empty() {
                    Ok(String::default())
                } else {
                    serde_json::to_string(&Subscription::Trades {
                        coin: trades.data[0].coin.clone(),
                    })
                    .map_err(|e| Error::JsonParse(e.to_string()))
                }
            }
            Message::L2Book(l2_book) => serde_json::to_string(&Subscription::L2Book {
                coin: l2_book.data.coin.clone(),
            })
            .map_err(|e| Error::JsonParse(e.to_string())),
            Message::Candle(candle) => serde_json::to_string(&Subscription::Candle {
                coin: candle.data.coin.clone(),
                interval: candle.data.interval.clone(),
            })
            .map_err(|e| Error::JsonParse(e.to_string())),
            Message::OrderUpdates(_) => Ok("orderUpdates".to_string()),
            Message::UserFundings(fundings) => serde_json::to_string(&Subscription::UserFundings {
                user: fundings.data.user,
            })
            .map_err(|e| Error::JsonParse(e.to_string())),
            Message::UserNonFundingLedgerUpdates(user_non_funding_ledger_updates) => {
                serde_json::to_string(&Subscription::UserNonFundingLedgerUpdates {
                    user: user_non_funding_ledger_updates.data.user,
                })
                .map_err(|e| Error::JsonParse(e.to_string()))
            }
            Message::Notification(_) => Ok("notification".to_string()),
            Message::WebData2(web_data2) => serde_json::to_string(&Subscription::WebData2 {
                user: web_data2.data.user,
            })
            .map_err(|e| Error::JsonParse(e.to_string())),
            Message::ActiveAssetCtx(active_asset_ctx) => {
                serde_json::to_string(&Subscription::ActiveAssetCtx {
                    coin: active_asset_ctx.data.coin.clone(),
                })
                .map_err(|e| Error::JsonParse(e.to_string()))
            }
            Message::ActiveSpotAssetCtx(active_spot_asset_ctx) => {
                serde_json::to_string(&Subscription::ActiveAssetCtx {
                    coin: active_spot_asset_ctx.data.coin.clone(),
                })
                .map_err(|e| Error::JsonParse(e.to_string()))
            }
            Message::ActiveAssetData(active_asset_data) => {
                serde_json::to_string(&Subscription::ActiveAssetData {
                    user: active_asset_data.data.user,
                    coin: active_asset_data.data.coin.clone(),
                })
                .map_err(|e| Error::JsonParse(e.to_string()))
            }
            Message::Bbo(bbo) => serde_json::to_string(&Subscription::Bbo {
                coin: bbo.data.coin.clone(),
            })
            .map_err(|e| Error::JsonParse(e.to_string())),
            Message::SubscriptionResponse | Message::Pong => Ok(String::default()),
            Message::NoData => Ok("".to_string()),
            Message::HyperliquidError(err) => Ok(format!("hyperliquid error: {err:?}")),
        }
    }

    async fn parse_and_send_data(
        data: std::result::Result<protocol::Message, tungstenite::Error>,
        subscriptions: &Arc<Mutex<HashMap<String, Vec<SubscriptionData>>>>,
    ) -> Result<()> {
        match data {
            Ok(data) => match data.into_text() {
                Ok(data) => {
                    if !data.starts_with('{') {
                        return Ok(());
                    }
                    let message = serde_json::from_str::<Message>(&data)
                        .map_err(|e| Error::JsonParse(e.to_string()))?;
                    let identifier = WsManager::get_identifier(&message)?;
                    if identifier.is_empty() {
                        return Ok(());
                    }

                    let mut subscriptions = subscriptions.lock().await;
                    let mut res = Ok(());
                    if let Some(subscription_datas) = subscriptions.get_mut(&identifier) {
                        for subscription_data in subscription_datas {
                            if let Err(e) = subscription_data
                                .sending_channel
                                .send(message.clone())
                                .map_err(|e| Error::WsSend(e.to_string()))
                            {
                                res = Err(e);
                            }
                        }
                    }
                    res
                }
                Err(err) => {
                    let error = Error::ReaderTextConversion(err.to_string());
                    Ok(WsManager::send_to_all_subscriptions(
                        subscriptions,
                        Message::HyperliquidError(error.to_string()),
                    )
                    .await?)
                }
            },
            Err(err) => {
                let error = Error::GenericReader(err.to_string());
                Ok(WsManager::send_to_all_subscriptions(
                    subscriptions,
                    Message::HyperliquidError(error.to_string()),
                )
                .await?)
            }
        }
    }

    async fn send_to_all_subscriptions(
        subscriptions: &Arc<Mutex<HashMap<String, Vec<SubscriptionData>>>>,
        message: Message,
    ) -> Result<()> {
        let mut subscriptions = subscriptions.lock().await;
        let mut res = Ok(());
        for subscription_datas in subscriptions.values_mut() {
            for subscription_data in subscription_datas {
                if let Err(e) = subscription_data
                    .sending_channel
                    .send(message.clone())
                    .map_err(|e| Error::WsSend(e.to_string()))
                {
                    res = Err(e);
                }
            }
        }
        res
    }

    async fn send_subscription_data(
        method: &'static str,
        writer: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, protocol::Message>,
        identifier: &str,
    ) -> Result<()> {
        let payload = serde_json::to_string(&SubscriptionSendData {
            method,
            subscription: &serde_json::from_str::<serde_json::Value>(identifier)
                .map_err(|e| Error::JsonParse(e.to_string()))?,
        })
        .map_err(|e| Error::JsonParse(e.to_string()))?;

        writer
            .send(protocol::Message::Text(payload))
            .await
            .map_err(|e| Error::Websocket(e.to_string()))?;
        Ok(())
    }

    async fn subscribe(
        writer: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, protocol::Message>,
        identifier: &str,
    ) -> Result<()> {
        Self::send_subscription_data("subscribe", writer, identifier).await
    }

    async fn unsubscribe(
        writer: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, protocol::Message>,
        identifier: &str,
    ) -> Result<()> {
        Self::send_subscription_data("unsubscribe", writer, identifier).await
    }

    pub(crate) async fn add_subscription(
        &mut self,
        identifier: String,
        sending_channel: UnboundedSender<Message>,
    ) -> Result<u32> {
        let mut subscriptions = self.subscriptions.lock().await;

        let identifier_entry = if let Subscription::UserEvents { user: _ } =
            serde_json::from_str::<Subscription>(&identifier)
                .map_err(|e| Error::JsonParse(e.to_string()))?
        {
            "userEvents".to_string()
        } else if let Subscription::OrderUpdates { user: _ } =
            serde_json::from_str::<Subscription>(&identifier)
                .map_err(|e| Error::JsonParse(e.to_string()))?
        {
            "orderUpdates".to_string()
        } else {
            identifier.clone()
        };
        let subscriptions = subscriptions
            .entry(identifier_entry.clone())
            .or_insert(Vec::new());

        if !subscriptions.is_empty() && identifier_entry.eq("userEvents") {
            return Err(Error::UserEvents);
        }

        if subscriptions.is_empty() {
            Self::subscribe(self.writer.lock().await.borrow_mut(), identifier.as_str()).await?;
        }

        let subscription_id = self.subscription_id;
        self.subscription_identifiers
            .insert(subscription_id, identifier.clone());
        subscriptions.push(SubscriptionData {
            sending_channel,
            subscription_id,
            id: identifier,
        });

        self.subscription_id += 1;
        Ok(subscription_id)
    }

    pub(crate) async fn remove_subscription(&mut self, subscription_id: u32) -> Result<()> {
        let identifier = self
            .subscription_identifiers
            .get(&subscription_id)
            .ok_or(Error::SubscriptionNotFound)?
            .clone();

        let identifier_entry = if let Subscription::UserEvents { user: _ } =
            serde_json::from_str::<Subscription>(&identifier)
                .map_err(|e| Error::JsonParse(e.to_string()))?
        {
            "userEvents".to_string()
        } else if let Subscription::OrderUpdates { user: _ } =
            serde_json::from_str::<Subscription>(&identifier)
                .map_err(|e| Error::JsonParse(e.to_string()))?
        {
            "orderUpdates".to_string()
        } else {
            identifier.clone()
        };

        self.subscription_identifiers.remove(&subscription_id);

        let mut subscriptions = self.subscriptions.lock().await;

        let subscriptions = subscriptions
            .get_mut(&identifier_entry)
            .ok_or(Error::SubscriptionNotFound)?;
        let index = subscriptions
            .iter()
            .position(|subscription_data| subscription_data.subscription_id == subscription_id)
            .ok_or(Error::SubscriptionNotFound)?;
        subscriptions.remove(index);

        if subscriptions.is_empty() {
            Self::unsubscribe(self.writer.lock().await.borrow_mut(), identifier.as_str()).await?;
        }
        Ok(())
    }
}

impl Drop for WsManager {
    fn drop(&mut self) {
        self.stop_flag.store(true, Ordering::Relaxed);
    }
}
