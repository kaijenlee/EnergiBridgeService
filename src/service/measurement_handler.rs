use crate::collect;
use crate::cpu;
use crate::util::{print_header, print_results, process_summary};
use jsonrpsee::types::error::INVALID_REQUEST_CODE;
use jsonrpsee::types::ErrorObjectOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::{stdout, Write};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use sysinfo::{ProcessExt, System, SystemExt};
use tokio::sync::mpsc;
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::sleep;

type MeasurementEntry = HashMap<String, MeasurementEntryValue>;

pub(crate) type Measurements = Vec<MeasurementEntry>;

#[derive(Debug, Clone, Deserialize)]
pub enum MeasurementEntryValue {
    Time(SystemTime),
    FunctionName(String),
    Measurement(f64),
    Delta(u128),
}

impl Serialize for MeasurementEntryValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        match self {
            MeasurementEntryValue::Time(time) => time
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
                .serialize(serializer),
            MeasurementEntryValue::FunctionName(name) => name.serialize(serializer),
            MeasurementEntryValue::Measurement(value) => value.serialize(serializer),
            MeasurementEntryValue::Delta(delta) => delta.serialize(serializer),
        }
    }
}

#[derive(Debug)]
pub struct MeasurementHandler {
    pub(crate) pid: u32,
    pub(crate) is_currently_measuring: Arc<AsyncMutex<bool>>,
    latest_results: Arc<AsyncMutex<Measurements>>,
    tx: AsyncMutex<Option<mpsc::Sender<()>>>,
    output_path: Option<String>,
    sep: Arc<String>,
    process_name: Arc<String>,
    summary: bool,
    current_fn_measured: AsyncMutex<Option<String>>,
}

impl MeasurementHandler {
    pub(crate) fn new(
        pid: u32,
        output_path: Arc<Option<String>>,
        sep: Arc<String>,
        summary: bool,
    ) -> Self {
        let mut system = System::new_all();
        system.refresh_all();
        // sysinfo uses Pid, which might be different types on different platforms
        let pid_ = sysinfo::Pid::from(pid as usize);

        // Get process name from pid
        let process_name = Arc::new(
            system
                .process(pid_)
                .map(|process| {
                    process
                        .exe()
                        .file_name()
                        .map(|f| f.to_str())
                        .flatten()
                        .unwrap_or(process.name())
                })
                .unwrap_or("UNKNOWN_PROCESS_NAME")
                .to_string(),
        );
        Self {
            pid,
            is_currently_measuring: Arc::new(AsyncMutex::new(false)),
            latest_results: Arc::new(AsyncMutex::new(Vec::new())),
            tx: AsyncMutex::new(None),
            output_path: {
                if let Some(ref output_path) = *output_path {
                    Some(format!("{}_{}", output_path, *process_name))
                } else {
                    None
                }
            },
            sep,
            process_name,
            summary,
            current_fn_measured: AsyncMutex::new(None),
        }
    }

    pub(crate) async fn measure(
        &self,
        function_name: String,
        collect_gpu: bool,
        interval: Duration,
    ) -> Result<(), String> {
        let (tx, mut stop_signal) = mpsc::channel::<()>(1);
        {
            let mut is_currently_measuring = self.is_currently_measuring.lock().await;
            if *is_currently_measuring {
                return Err(format!(
                    "A measurement session is already ongoing for pid: {}.",
                    self.pid
                ));
            }
            *is_currently_measuring = true;
            // Create a new session
            let mut measurements_lock = self.latest_results.lock().await;
            *measurements_lock = Vec::new();
            let mut current_fn_measured = self.current_fn_measured.lock().await;
            *current_fn_measured = Some(function_name.clone());


            {
                let mut tx_lock = self.tx.lock().await;
                *tx_lock = Some(tx.clone());
            }
        }

        
        #[cfg(not(target_os = "macos"))]
        cpu::msr::start_rapl();
        let mut sys = System::new_all();
        sys.refresh_all();
        sleep(System::MINIMUM_CPU_UPDATE_INTERVAL).await;
        let mut results: HashMap<String, f64> = HashMap::new();
        collect(&mut sys, collect_gpu, self.pid, &mut results);
        let mut previous_time = SystemTime::now();
        let mut energy_array: f64 = 0f64;
        let start_instant = Instant::now();
        let start_time = Arc::new(
            previous_time
                .clone()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        );
        let mut previous_results = results.clone();

        self.print_header(&results, function_name.clone(), *start_time.clone());

        loop {
            tokio::select! {
                _ = stop_signal.recv() => {
                    if !results.is_empty() {
                        self.print_results(previous_time, &mut results, function_name.clone(), *start_time.clone());
                        energy_array += process_summary(
                            self.summary,
                            &mut results,
                            &mut previous_time,
                            &mut previous_results,
                        );
                        self.process_last_measurement(previous_time, &previous_results).await;
                        if energy_array > 0.0 && self.summary {
                            println!(
                                "Energy consumption in joules: {} for {} sec of execution for application {} with function {}.",
                                energy_array,
                                start_instant.elapsed().as_secs_f32(),
                                self.process_name,
                                function_name
                            );
                         }
                    }
                    {
                        let mut is_currently_measuring = self.is_currently_measuring.lock().await;
                        *is_currently_measuring = false;
                    }
                    break;
                },
                _ =  async {
                    let time_before = SystemTime::now();
                    self.print_results(previous_time, &mut results, function_name.clone(), *start_time.clone());
                    energy_array += process_summary(
                        self.summary,
                        &mut results,
                        &mut previous_time,
                        &mut previous_results,
                     );
                    self.process_last_measurement(previous_time, &previous_results).await;

                    previous_time = SystemTime::now();
                    previous_results = results.clone();
                    collect(&mut sys, collect_gpu, self.pid, &mut results);

                    sleep(interval - time_before.elapsed().unwrap()).await;
                }=> continue
            }
        }

        Ok(())
    }

    fn print_header(
        &self,
        results: &HashMap<String, f64>,
        function_name: String,
        start_time: u128,
    ) {
        let mut output = self.get_output(function_name, start_time);
        output
            .write_all(format!("Application{}Function{}", self.sep, self.sep).as_bytes())
            .expect("Failed to write header");
        print_header(results, self.sep.as_str(), &mut *output);
    }

    fn get_output(&self, function_name: String, start_time: u128) -> Box<dyn Write> {
        match &self.output_path {
            Some(ref path) => {
                let path_name = format!("{}_{}_{}", path, function_name, start_time);
                Box::new(File::create(path_name).expect("Failed to open output file"))
                    as Box<dyn Write>
            }
            None => Box::new(stdout()) as Box<dyn Write>,
        }
    }

    fn print_results(
        &self,
        time: SystemTime,
        results: &mut HashMap<String, f64>,
        function_name: String,
        start_time: u128,
    ) {
        let mut output = self.get_output(function_name.clone(), start_time);
        output
            .write_all(
                format!(
                    "{}{}{}{}",
                    self.process_name, self.sep, function_name, self.sep
                )
                .as_bytes(),
            )
            .expect("Failed to write header");
        print_results(time, results, self.sep.as_str(), &mut *output);
    }

    async fn process_last_measurement(
        &self,
        previous_time: SystemTime,
        results: &HashMap<String, f64>,
    ) {
        let mut latest_result = self.latest_results.lock().await;
        //Flush to results
        let mut entry: MeasurementEntry = HashMap::new();
        entry.insert(
            "TIME".to_string(),
            MeasurementEntryValue::Time(previous_time),
        );
        entry.insert(
            "DELTA".to_string(),
            MeasurementEntryValue::Delta(previous_time.elapsed().unwrap().as_millis()),
        );
        entry.extend(
            results
                .iter()
                .map(|(k, v)| (k.clone(), MeasurementEntryValue::Measurement(v.clone()))),
        );
        latest_result.push(entry);
    }
    pub(crate) async fn stop_measurement(
        &self,
        function_name: String,
    ) -> Result<Measurements, ErrorObjectOwned> {
        {
            let is_currently_measuring = *self.is_currently_measuring.lock().await;
            if !is_currently_measuring {
                return Err(ErrorObjectOwned::owned(
                    INVALID_REQUEST_CODE,
                    format!("No measurement session is ongoing for pid: {}.", self.pid),
                    None::<()>,
                ));
            }
        }
        {
            let current_fn_measured = self.current_fn_measured.lock().await;
            match *current_fn_measured {
                Some(ref current_fn_measured) => {
                    if current_fn_measured != &function_name {
                        return Err(ErrorObjectOwned::owned(
                            INVALID_REQUEST_CODE,
                            format!(
                                "Current measurements involves a different function: {}.",
                                current_fn_measured
                            ),
                            None::<()>,
                        ));
                    }
                }

                None => {
                    return Err(ErrorObjectOwned::owned(
                        INVALID_REQUEST_CODE,
                        "No function is currently being measured.".to_string(),
                        None::<()>,
                    ));
                }
            }
        }

        let tx_clone = {
            let tx_lock = self.tx.lock().await;
            tx_lock.clone()
        };

        if let Some(tx) = tx_clone {
            tx.try_send(()).expect("Failed to send stop signal");
        }

        // Indication that the last measurement has been processed and added to the results
        loop {
            let is_currently_measuring = *self.is_currently_measuring.lock().await;
            if !is_currently_measuring {
                break;
            }
            // Short sleep to avoid busy waiting
            sleep(Duration::from_millis(10)).await;
        }

        let latest_result_lock = self.latest_results.lock().await;

        Ok(latest_result_lock.clone())
    }
}
