# Time Series Database System Tester

This repository contains the implementation of a modular and extensible test system for the automated evaluation of time series database systems (TSDBSs). The system was developed as part of a thesis comparing InfluxDB, TimescaleDB, and MongoDB based on both conceptual analysis and experimental benchmarking. It supports multiple real-world scenarios, including sensor data processing in smart homes, large-scale taxi data analysis, and monitoring of short-lived services. The framework measures key performance metrics such as query latency, resource utilization, and energy consumption, providing an basis to evaluate the databases. The modular design allows easy integration of additional databases and test scenarios, making it a flexible tool for further research and development.

## Results

The raw csv files can be found in the `results` directory.

## Setting Up the Test System

To set up the test system, follow these steps:

### 1. Install Dependencies

The following packages must be installed to run the test system¹:

- `libvirt`
- `qemu-full`
- `rust`
- `python-pandas`
- `python-pyarrow`
- `virt-manager` (optional)

Additionally, either the `kvm_amd` or `kvm_intel` kernel module must be loaded, depending on the processor used.

<small>[1]: The test system was developed using Arch Linux. Package names may differ for other distributions.</small>

### 2. Download the Project

The project files, including raw result data, can be downloaded via Git from:  
[`https://github.com/Malex14/time-series-database-system-tester`](https://github.com/Malex14/time-series-database-system-tester)

```sh
git clone https://github.com/Malex14/time-series-database-system-tester.git
```

### 3. Download Dataset for Scenario B

For Scenario B, the required dataset must be downloaded from [https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) in parquet format². The dataset must then be preprocessed using the Python script `convert_dataset_scenario_b.py`. The script reads files from the specified folder and writes the processed data to:

```text
./dataset/scenario_b
```

<small>[2]: The experiments in this thesis used data from January 2011 to November 2024.</small>

### 4. Download Debian Disk Image

The required Debian disk image must be downloaded from:
[https://cloud.debian.org/images/cloud/bookworm/latest/](https://cloud.debian.org/images/cloud/bookworm/latest/)

The file name is: `debian-12-genericcloud-amd64.qcow2`

### 5. Configure the Project

The configuration file `vm_config.toml` must be edited to set the `temp_dir` option to an existing directory. This directory will store temporary files used by the test system. It should be located on a high-speed storage device, as it will contain virtual machine disk images.

Additionally, the `base_disk_image` option must be set to the path of the previously downloaded Debian image.

### 6. Compile the Project

To compile the project, run the following command:

```sh
cargo build --release --workspace
```

The compiled executable `instrumentation_gatherer` will be located in:

```text
./target/release/
```

This executable handles metric collection in the test environment and must be copied to the `bin` directory within the project folder.

### 7. Adjust File Permissions

For energy consumption measurements, execute the following commands to adjust file permissions:

```sh
sudo chmod +r /sys/class/powercap/intel-rapl:0/energy_uj
sudo chmod +r /sys/class/powercap/intel-rapl:0:0/energy_uj
```

### 8. Run the Test System

To start the test system, execute:

```sh
./target/release/database_tester -t [number of query executions] -s [scenario]
```

Example:

```sh
./target/release/database_tester -t 10 -s ScenarioB
```

The results will be stored as CSV files in the out directory.
