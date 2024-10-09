import random
import mysql.connector
from faker import Faker
from datetime import date

# Initialize Faker with Nigerian localization
fake = Faker('yo_NG')

# MySQL database connection configuration
db_config = {
    'host': 'host_name',
    'user': 'user',
    'password': 'password',
    'database': 'databaseName',
    'port': 'port'
}

# Number of students to generate
num_students = 2800  # Adjust as needed

# Richness and seriousness categories
richness_categories = ['Poor', 'Middle Class', 'Rich']
seriousness_categories = ['Serious', 'Moderate', 'Unserious']

# Class levels (JSS1-SSS3)
class_levels = ['JSS1', 'JSS2', 'JSS3', 'SSS1', 'SSS2', 'SSS3']

# Sections (A to F)
sections = ['A', 'B', 'C', 'D', 'E', 'F']

# Gender options
gender_options = ['Male', 'Female', 'Other']

# Function to generate a biased attendance rate based on richness and seriousness
def generate_attendance_rate(richness, seriousness):
    if richness == 'Rich' and seriousness == 'Serious':
        return round(random.uniform(0.85, 1.0), 2)
    elif richness == 'Middle Class' and seriousness in ['Serious', 'Moderate']:
        return round(random.uniform(0.75, 0.9), 2)
    elif richness == 'Poor' or seriousness == 'Unserious':
        return round(random.uniform(0.5, 0.8), 2)
    else:
        return round(random.uniform(0.6, 0.85), 2)

# Function to generate a biased assessment score based on richness and seriousness
def generate_assessment_score(richness, seriousness):
    if richness == 'Rich' and seriousness == 'Serious':
        return random.randint(75, 100)
    elif richness == 'Middle Class' and seriousness in ['Serious', 'Moderate']:
        return random.randint(60, 85)
    elif richness == 'Poor' or seriousness == 'Unserious':
        return random.randint(30, 60)
    else:
        return random.randint(40, 70)

import random
from datetime import datetime

def enrollment_date(age, class_level, seriousness):
    # Get the current year
    current_year = datetime.now().year

    # Define class differences
    class_diff_mapping = {
        'JSS2': -1,
        'JSS3': -2,
        'SSS1': -3,
        'SSS2': -4,
        'SSS3': -5
    }

    # Constant enrollment date for JSS1
    if class_level.startswith("JSS1"):
        return datetime(year=2024, month=9, day=16).date()

    # Get class_diff based on class_level
    class_diff = class_diff_mapping.get(class_level, 0)

    # Check specific conditions for age and seriousness
    if class_level.startswith("JSS2") and seriousness == "Unserious" and age in [13, 14]:
        class_diff = -1  # Override class_diff for this condition

    # Generate random day between 10 and 25 for enrollment date
    random_day = random.randint(10, 25)

    # Calculate the enrollment date
    enrollment_year = current_year + class_diff
    return datetime(year=enrollment_year, month=9, day=random_day).date()


def generate_age(class_level):
    if class_level.startswith("JSS1"):
        # Dominant ages: 11, 12 with a 2% chance of 10, 13
        return random.choices([11, 12], weights=[98, 98], k=1)[0] if random.random() > 0.02 else random.choice([10, 13])

    elif class_level.startswith("JSS2"):
        # Dominant ages: 12, 13 with a 5% chance of 11, 14
        return random.choices([12, 13], weights=[95, 95], k=1)[0] if random.random() > 0.05 else random.choice([11, 14])

    elif class_level.startswith("JSS3"):
        # Dominant ages: 13, 14 with a 5% chance of 12, 15
        return random.choices([13, 14], weights=[95, 95], k=1)[0] if random.random() > 0.05 else random.choice([12, 15])

    elif class_level.startswith("SSS1"):
        # Dominant ages: 14, 15 with a 5% chance of 16 and a 2% chance of 13
        return random.choices([14, 15], weights=[95, 95], k=1)[0] if random.random() > 0.05 else random.choice([16, 13])

    elif class_level.startswith("SSS2"):
        # Dominant ages: 15, 16 with a 5% chance of 15 and a 2% chance of 12
        return random.choices([15, 16], weights=[95, 95], k=1)[0] if random.random() > 0.05 else random.choice([15, 12])

    elif class_level.startswith("SSS3"):
        # Dominant ages: 16, 17 with a 3% chance of 18 and a 2% chance of 15
        return random.choices([16, 17], weights=[97, 97], k=1)[0] if random.random() > 0.03 else random.choice([18, 15])

    else:
        raise ValueError("Invalid class level provided.")

# Generate fake student data and insert into MySQL
# Generate fake student data and insert into MySQL
try:
    # Establish database connection
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()

    # SQL query to insert data into the students_data table
    insert_query = """
    INSERT INTO students_data (
        first_name, last_name, class_level, attendance_rate, gender, age, address, enrollment_date,
        assessment_score, parent_income, seriousness
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    # Generate data and insert into the database
    for _ in range(num_students):
        first_name = fake.first_name()
        last_name = fake.last_name()
        # Add a random section (A-F) to the class level
        class_level = f"{random.choice(class_levels)}{random.choice(sections)}"
        attendance_rate = generate_attendance_rate(random.choice(richness_categories), random.choice(seriousness_categories))
        gender = random.choice(gender_options)
        age = generate_age(class_level)  # Adjust age based on class level (JSS and SSS range)
        address = fake.address().replace('\n', ', ')
        enrollment_dt = enrollment_date(age, class_level, seriousness)  # Random enrollment date in the last 3 years
        assessment_score = generate_assessment_score(random.choice(richness_categories), random.choice(seriousness_categories))
        parent_income = random.choice(richness_categories)
        seriousness = random.choice(seriousness_categories)

        # Insert the generated student record into the database
        cursor.execute(insert_query, (
            first_name, last_name, class_level, attendance_rate, gender, age, address, enrollment_dt,
            assessment_score, parent_income, seriousness
        ))

    # Commit the transaction
    connection.commit()
    print(f"{num_students} students inserted into the database successfully!")

except mysql.connector.Error as err:
    print(f"Error: {err}")
finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection closed.")

