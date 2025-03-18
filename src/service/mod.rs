use crate::service::server::{MeasurementRpcServer, RpcServer};
use futures_util::FutureExt;
use jsonrpsee::server::{
    http, serve_with_graceful_shutdown, stop_channel, ws, ConnectionGuard, ConnectionState,
    RpcServiceBuilder, ServerConfig, ServerHandle, StopHandle,
};
use jsonrpsee::Methods;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;

mod measurement_handler;
mod server;

pub(crate) async fn run_server(
    collect_gpu: bool,
    interval: Duration,
    sep: &str,
    output_path: Option<String>,
    summary: bool,
    port: u16,
) -> anyhow::Result<ServerHandle> {
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], port))).await?;
    let (stop_handle, server_handle) = stop_channel();
    let measurement_server =
        MeasurementRpcServer::new(collect_gpu, interval, sep, output_path, summary);

    #[derive(Clone)]
    struct PerConnection {
        methods: Methods,
        stop_handle: StopHandle,
        conn_id: Arc<AtomicU32>,
        conn_guard: ConnectionGuard,
    }

    let per_conn = PerConnection {
        methods: measurement_server.into_rpc().into(),
        stop_handle: stop_handle.clone(),
        conn_id: Default::default(),
        conn_guard: ConnectionGuard::new(Semaphore::MAX_PERMITS),
    };

    tokio::spawn(async move {
        loop {
            // The `tokio::select!` macro is used to wait for either of the
            // listeners to accept a new connection or for the server to be
            // stopped.
            let (sock, _) = tokio::select! {
                res = listener.accept() => {
                    match res {
                        Ok(sock) => sock,
                        Err(e) => {
                            tracing::error!("failed to accept v4 connection: {:?}", e);
                            continue;
                        }
                    }
                }
                _ = per_conn.stop_handle.clone().shutdown() => break,
            };
            let per_conn = per_conn.clone();

            // Create a service handler.
            let stop_handle2 = per_conn.stop_handle.clone();
            let per_conn = per_conn.clone();
            let svc = tower::service_fn(move |req| {
                let PerConnection {
                    methods,
                    stop_handle,
                    conn_guard,
                    conn_id,
                } = per_conn.clone();

                // jsonrpsee expects a `conn permit` for each connection.
                let Some(conn_permit) = conn_guard.try_acquire() else {
                    return async { Ok::<_, Infallible>(http::response::too_many_requests()) }
                        .boxed();
                };

                if !ws::is_upgrade_request(&req) {
                    let rpc_service = RpcServiceBuilder::new();

                    let server_cfg = ServerConfig::default();
                    let conn = ConnectionState::new(
                        stop_handle,
                        conn_id.fetch_add(1, Ordering::Relaxed),
                        conn_permit,
                    );

                    // There is another API for making call with just a service as well.
                    //
                    // See [`jsonrpsee::server::http::call_with_service`]
                    async move {
                        // Rpc call finished successfully.
                        let res = http::call_with_service_builder(
                            req,
                            server_cfg,
                            conn,
                            methods,
                            rpc_service,
                        )
                        .await;
                        Ok(res)
                    }
                    .boxed()
                } else {
                    async { Ok(http::response::denied()) }.boxed()
                }
            });

            // Upgrade the connection to an HTTP service.
            tokio::spawn(serve_with_graceful_shutdown(
                sock,
                svc,
                stop_handle2.shutdown(),
            ));
        }
    });

    Ok(server_handle)
}
