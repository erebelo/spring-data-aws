import csv
from faker import Faker
import random
import string

# Initialize Faker
fake = Faker('en_US')

# Define the number of records
num_records = 5000

# Define the CSV file name
csv_file_name = 'legacy_addresses.csv'

# Define the columns
columns = ['id', 'address_type', 'address_line_1', 'address_line_2', 'city', 'state', 'zip_code', 'country']

# Define address types
address_types = ['HOME', 'WORK', 'OTHER']

# Function to generate a random string-numeric ID
def generate_random_id():
    # Generate a random string (2 letters) and a random number (4 digits)
    letters = ''.join(random.choices(string.ascii_uppercase, k=2))
    numbers = ''.join(random.choices(string.digits, k=4))
    return f"{letters}{numbers}"

# Generate a pool of unique IDs
id_pool = [generate_random_id() for _ in range(num_records)]  # Larger pool to reduce reuse

# Track used address types for each ID
id_address_types = {id: set() for id in id_pool}

# Open the CSV file for writing
with open(csv_file_name, mode='w', newline='') as file:
    writer = csv.writer(file)

    # Write the header
    writer.writerow(columns)

    # Track the total number of records
    total_records = 0

    # Generate and write the data
    while total_records < num_records:
        # Randomly select an ID from the pool
        id = random.choice(id_pool)

        # Get the address types already used for this ID
        used_types = id_address_types[id]

        # If all 3 address types are already used, generate a new ID
        if len(used_types) >= 3:
            id = generate_random_id()
            id_pool.append(id)  # Add the new ID to the pool
            used_types = set()  # Reset for the new ID
            id_address_types[id] = used_types

        # Randomly decide how many address types to assign to this ID (1, 2, or 3)
        if len(used_types) == 0:
            # If no address types are used yet, randomly choose 1, 2, or 3
            # Make it more likely to choose 1 to balance the distribution
            num_address_types = random.choices([1, 2, 3], weights=[70, 20, 10], k=1)[0]
        else:
            # If some address types are already used, limit to the remaining types
            num_address_types = random.randint(1, 3 - len(used_types))

        # Ensure we don't exceed the total number of records
        if total_records + num_address_types > num_records:
            num_address_types = num_records - total_records

        # Choose address types that haven't been used yet for this ID
        available_types = set(address_types) - used_types
        selected_types = random.sample(list(available_types), num_address_types)

        # Generate records for the selected address types
        for address_type in selected_types:
            # Generate address data
            address_line_1 = fake.street_address()
            address_line_2 = fake.secondary_address() if random.choice([True, False]) else ''
            city = fake.city()
            state = fake.state_abbr()
            zipcode = fake.zipcode()
            country = 'US'

            # Write the row
            writer.writerow([id, address_type, address_line_1, address_line_2, city, state, zipcode, country])

            # Add the selected address type to the used types for this ID
            used_types.add(address_type)
            id_address_types[id] = used_types

            # Increment the total number of records
            total_records += 1

print(f"CSV file '{csv_file_name}' with {total_records} records has been generated.")