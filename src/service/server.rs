use crate::service::measurement_handler::{MeasurementHandler, Measurements};
use jsonrpsee::core::async_trait;
use jsonrpsee::proc_macros::rpc;
use jsonrpsee::types::error::INVALID_REQUEST_CODE;
use jsonrpsee::types::ErrorObjectOwned;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex as AsyncMutex;

#[rpc(server)]
pub trait Rpc {
    #[method(name = "start_measurements")]
    async fn start_measurements(
        &self,
        pid: u32,
        function_name: String,
    ) -> Result<bool, ErrorObjectOwned>;

    #[method(name = "stop_measurements")]
    async fn stop_measurements(
        &self,
        pid: u32,
        function_name: String,
    ) -> Result<Measurements, ErrorObjectOwned>;
}

#[derive(Debug)]
pub struct MeasurementRpcServer {
    measurement_handler: AsyncMutex<Option<Arc<MeasurementHandler>>>,
    collect_gpu: bool,
    interval: Duration,
    sep: Arc<String>,
    output_path: Arc<Option<String>>,
    summary: bool,
}

impl MeasurementRpcServer {
    pub(crate) fn new(
        collect_gpu: bool,
        interval: Duration,
        sep: &str,
        output_path: Option<String>,
        summary: bool,
    ) -> Self {
        Self {
            measurement_handler: AsyncMutex::new(None),
            collect_gpu,
            interval,
            sep: Arc::new(sep.to_string()),
            output_path: Arc::new(output_path),
            summary,
        }
    }

    async fn get_handler(
        &self,
        pid: u32,
        handlers_lock: &mut Option<Arc<MeasurementHandler>>,
    ) -> Result<Arc<MeasurementHandler>, ErrorObjectOwned> {
        match &*handlers_lock {
            // If we have an existing handler
            Some(existing_handler) => {
                // Check if it's currently measuring
                if *existing_handler.is_currently_measuring.lock().await {
                    return Err(ErrorObjectOwned::owned(
                        INVALID_REQUEST_CODE,
                        format!(
                            "A measurement is already in progress for pid: {}.",
                            existing_handler.pid
                        ),
                        None::<()>,
                    ));
                }

                // Reuse the existing handler if the pid matches, otherwise create a new one
                if existing_handler.pid == pid {
                    Ok(existing_handler.clone())
                } else {
                    Ok(Arc::new(MeasurementHandler::new(
                        pid,
                        self.output_path.clone(),
                        self.sep.clone(),
                        self.summary,
                    )))
                }
            }
            // No existing handler, create a new one
            None => Ok(Arc::new(MeasurementHandler::new(
                pid,
                self.output_path.clone(),
                self.sep.clone(),
                self.summary,
            ))),
        }
    }
}

#[async_trait]
impl RpcServer for MeasurementRpcServer {
    async fn start_measurements(
        &self,
        pid: u32,
        function_name: String,
    ) -> Result<bool, ErrorObjectOwned> {
        // Get a mutable reference to the handler or create a new one
        let mut handlers_lock = self.measurement_handler.lock().await;

        // Check if a handler already exists for this pid or create a new one
        let handler = match self.get_handler(pid, &mut handlers_lock).await {
            Ok(value) => value,
            // Return the error if there is currently an ongoing measurement
            Err(value) => return Err(value),
        };

        *handlers_lock = Some(handler.clone());

        drop(handlers_lock);

        // Clone necessary values before moving to the spawned task
        let fn_name = function_name;
        let collect_gpu = self.collect_gpu;
        let interval = self.interval;

        // Clone the handler to send to another task
        let handler_clone = handler.clone(); // Assuming MeasurementHandler implements Clone
        tokio::spawn(async move {
            handler_clone
                .measure(fn_name, collect_gpu, interval)
                .await
                .or_else(|e| {
                    return Err(ErrorObjectOwned::owned(
                        INVALID_REQUEST_CODE,
                        format!("Measurement session failed for pid: {pid} due to {e}"),
                        None::<()>,
                    ));
                })
        });

        Ok(true)
    }

    async fn stop_measurements(
        &self,
        pid: u32,
        function_name: String,
    ) -> Result<Measurements, ErrorObjectOwned> {
        let handlers_lock = self.measurement_handler.lock().await;

        // Get a reference to the handler
        let Some(handler) = handlers_lock.as_ref().filter(|&h| h.pid == pid) else {
            return Err(ErrorObjectOwned::owned(
                INVALID_REQUEST_CODE,
                format!("No ongoing measurement was made for pid: {pid}."),
                None::<()>,
            ));
        };

        // Stop measurement and return the result
        let result = handler.stop_measurement(function_name).await?;

        Ok(result)
    }
}
