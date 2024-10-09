import random
import mysql.connector
from faker import Faker

# Initialize Faker
fake = Faker()

# MySQL connection details
db_config = {
    'host': 'host_name',
    'user': 'user',
    'password': 'password',
    'database': 'databaseName',
    'port': 'port'
}

# Batch size for inserts
BATCH_SIZE = 500

# Connect to MySQL database
conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

# Step 1: Extract data from the 'students_data' table
student_query = "SELECT students_id, last_name FROM students_data"
cursor.execute(student_query)
students_data = cursor.fetchall()

# Convert students_data to a dictionary for easy processing
students_dict = {}
for student_id, last_name in students_data:
    if last_name not in students_dict:
        students_dict[last_name] = []
    students_dict[last_name].append(student_id)

# Step 2: Process grouped students based on last names (limit 3 per guardian)
guardian_data = []
student_guardian_data = []

guardian_id_counter = 1000
student_guardian_id_counter = 1000

# Track progress
total_students = len(students_data)
processed_students = 0

print(f"Total number of students: {total_students}")

for last_name, student_ids in students_dict.items():
    # Split student_ids into chunks of 3 students per guardian
    for i in range(0, len(student_ids), 3):
        current_group = student_ids[i:i + 3]  # Limit each group to a maximum of 3 students

        # Generate guardian information
        first_name = fake.first_name()
        phone_number = f"+234{random.choice([70, 80, 81, 90, 91])}{fake.random_number(digits=8)}"

        # Insert guardian data
        guardian_data.append((
            guardian_id_counter,
            first_name,
            last_name,
            phone_number
        ))

        # Assign guardian to each student in the group
        for index, student_id in enumerate(current_group):
            relationship = random.choice(['Mother', 'Father'])
            is_primary = index == 0  # Set the first student as primary for the guardian

            # Add student-guardian relationship data
            student_guardian_data.append((
                student_guardian_id_counter,
                guardian_id_counter,
                student_id,
                relationship,
                is_primary
            ))

            student_guardian_id_counter += 1

        # Increment the guardian ID for the next iteration
        guardian_id_counter += 1

        # Track progress
        processed_students += len(current_group)

        # Insert in batches
        if processed_students % BATCH_SIZE == 0:
            # Step 3: Insert batch data into the Guardian table
            guardian_insert_query = """
                INSERT INTO Guardian (guardian_id, first_name, last_name, phone_number)
                VALUES (%s, %s, %s, %s)
            """
            cursor.executemany(guardian_insert_query, guardian_data)

            # Step 4: Insert batch data into the StudentGuardian table
            student_guardian_insert_query = """
                INSERT INTO StudentGuardian (student_guardian_id, guardian_id, student_id, relationship, is_primary)
                VALUES (%s, %s, %s, %s, %s)
            """
            cursor.executemany(student_guardian_insert_query, student_guardian_data)

            # Clear the batch data after insertion
            guardian_data.clear()
            student_guardian_data.clear()

            # Commit the batch
            conn.commit()

            # Show progress
            print(f"Processed {processed_students}/{total_students} students...")

# Insert any remaining data after the loop
if guardian_data or student_guardian_data:
    # Step 3: Insert remaining Guardian data
    guardian_insert_query = """
        INSERT INTO Guardian (guardian_id, first_name, last_name, phone_number)
        VALUES (%s, %s, %s, %s)
    """
    cursor.executemany(guardian_insert_query, guardian_data)

    # Step 4: Insert remaining StudentGuardian data
    student_guardian_insert_query = """
        INSERT INTO StudentGuardian (student_guardian_id, guardian_id, student_id, relationship, is_primary)
        VALUES (%s, %s, %s, %s, %s)
    """
    cursor.executemany(student_guardian_insert_query, student_guardian_data)

    # Commit the final batch
    conn.commit()

    # Show final progress
    print(f"Processed {processed_students}/{total_students} students (Final batch)")

# Commit the transactions and close connection
cursor.close()
conn.close()

print("Guardian and StudentGuardian data has been successfully inserted.")
