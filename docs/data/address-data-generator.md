# Address Data Generator

The `generate_address_data.py` script generates fake address data using the `Faker` library in Python.

## Description

This script is structured to create realistic-looking addresses, including the following fields:

- **id** (string-numeric)
- **addresstype** (`HOME`, `WORK`, `OTHER`)
- **addressline1**
- **addressline2**
- **city**
- **state**
- **country**
- **zipcode**

Each `id` can have up to three associated records (one per `addresstype`: `HOME`, `WORK`, `OTHER`), ensuring a balanced distribution. The total number of records is configurable, and the script dynamically adjusts to stay within the specified limit.

## Requirements

- Python

## Installation

To install the `Faker` dependency, run the following command:

```sh
pip install faker
```

## Usage

To generate address data, execute the script:

```sh
python generate_address_data.py
```
