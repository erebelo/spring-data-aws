# Address Data Generator

The `generate_address_data.py` script generates fake address data using the `Faker` library in Python.

## Description

This script is structured to create realistic-looking addresses, including the following fields:

- **id** (string-numeric)
- **address_type** (`HOME`, `WORK`, `OTHER`)
- **address_line_1**
- **address_line_2**
- **city**
- **state**
- **zip_code**
- **country**

Each `id` can have up to three associated records (one per `address_type`: `HOME`, `WORK`, `OTHER`), ensuring a balanced distribution. The total number of records is configurable, and the script dynamically adjusts to stay within the specified limit.

## Requirements

- Python

## Installation

1. **Install Python** (if not already installed):

   - Download the latest Python installer for Windows from [https://www.python.org/downloads/windows/](https://www.python.org/downloads/windows/)
   - Run the installer and **make sure to check “Add Python to PATH”**
   - Verify installation by opening Command Prompt and running:
     ```sh
     python --version
     ```
     You should see a version like `Python 3.x.x`.

2. **Install the Faker library**:
   ```sh
   pip install faker
   ```

## Usage

To generate address data, execute the script:

```sh
 python generate_address_data.py
```
