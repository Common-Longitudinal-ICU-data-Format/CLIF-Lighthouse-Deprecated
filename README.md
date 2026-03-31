# CLIF-Lighthouse [![No Maintenance Intended](http://unmaintained.tech/badge.svg)](http://unmaintained.tech/)
_Deprecated and migrated to TableOne_
## Setup Instructions

### :warning: Deactivate Any Currently Active Environment(s) :warning:

Before proceeding, deactivate any existing virtual environment:

```sh
deactivate
```
#### and/or
```sh
conda deactivate
```

## A. Environment setup

The environment setup code is provided in the setup.sh file for macOS and setup.bat for Windows.

### For macOS:

#### 1. Make the `setup.sh` script executable
In the command line, navigate to the directory where the setup.sh file is located and run:
```sh
chmod +x setup.sh
```

#### 2. Run the script
```sh
./setup.sh
```

### For Windows:

Run the script
```sh
setup.bat
```

## B. Application Launch
To start the application navigate to the app directory in the terminal by executing the following command:

```
source .clif_lighthouse/bin/activate
cd app
streamlit run app.py
```

## CLIF-Lighthouse - Quality Control
<img width="1311" alt="Screenshot 2025-01-23 at 11 05 53" src="https://github.com/user-attachments/assets/2741f5c8-08f6-4edc-a238-bf6c5e0a8c2a" />


