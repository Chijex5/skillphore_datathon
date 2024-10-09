import mysql.connector
import random
from datetime import datetime

# MySQL database connection configuration
db_config = {
    'host': 'host_name',
    'user': 'user',
    'password': 'password',
    'database': 'databaseName',
    'port': 'port'
}

# Define class differences (used to check how many times the student repeated)
class_diff_mapping = {
    'JSS2': 1,
    'JSS3': 2,
    'SSS1': 3,
    'SSS2': 4,
    'SSS3': 5
}

# Junior Secondary School Subjects (JSS)
jss_subjects = [
    "Mathematics", "English Language", "Yoruba", "Basic Science", "Social Studies",
    "Fine Arts/Creative Art", "Agricultural Science", "Civic Education", "Christian Religion Studies",
    "Physical and Health Education", "Business Studies", "French", "Computer Studies",
    "Home Economics", "Music", "Basic Technology"
]

sss_core_subjects = ["English Language", "Mathematics", "Civic Education"]
sss_science_core_subjects = ["Physics", "Chemistry", "Biology"]
sss_pure_science_subjects = ["Further Mathematics", "Agricultural Science", "Computer Science"]
sss_technical_subjects = ["Technical Drawing", "Food and Nutrition", "Agricultural Science"]
sss_commercial_subjects = ["Financial Account", "Book Keeping", "Typewriting", "Office Practice", "Commerce", "Data Processing"]
sss_arts_subjects = ["Economics", "Government", "Literature – in- English", "Christian Religion Knowledge", "Fine Art/Creative Art", "French", "Geography"]

# SURFIX options
SURFIX = ["A", "B", "C", "D", "E", "F"]

# Function to assign a score with an 8% probability for scores above 90
def assign_score():
    if random.random() < 0.08:  # 8% chance to score above 90
        return round(random.uniform(90, 100), 2)
    return round(random.uniform(45, 90), 2)

# Function to insert exam history in batch from current class level back to JSS1
# Adjusted version of the function to handle the backdate logic
def insert_exam_history_batch(student_id, enrollment_year, current_class_level, cursor, exam_history_batch):
    # Define the class levels in descending order
    class_levels = ['SSS3', 'SSS2', 'SSS1', 'JSS3', 'JSS2', 'JSS1']
    surfixes = ["A", "B", "C", "D", "E", "F"]

    # Extract the base class level (e.g., 'SSS1' from 'SSS1A')
    class_level_base = current_class_level[:4] if current_class_level.startswith('SSS') else current_class_level[:4]
    suffix = current_class_level[-1]

    # Validate suffix
    if suffix not in surfixes:
        print(f"Invalid suffix for class level: {suffix}")
        return

    # Find the index of the base class level in the list
    try:
        start_index = class_levels.index(class_level_base)
    except ValueError:
        print(f"Class level {class_level_base} not found in class_levels list.")
        return

    # Determine the backdate levels based on whether it's SSS or JSS
    if class_level_base.startswith("SSS"):
        backdate_levels = [f"SSS3{suffix}", f"SSS2{suffix}", f"SSS1{suffix}"]
        random_jss_suffix = random.choice(surfixes)  # Pick random suffix for JSS backdating
        backdate_levels += [f"JSS3{random_jss_suffix}", f"JSS2{random_jss_suffix}", f"JSS1{random_jss_suffix}"]
    elif class_level_base.startswith("JSS"):
        backdate_levels = [f"JSS3{suffix}", f"JSS2{suffix}", f"JSS1{suffix}"]
    else:
        print("Invalid class level format.")
        return

    # Subject allocation based on suffix
    def get_subjects(class_level):
        if class_level.startswith("JSS"):
            return jss_subjects
        elif class_level.startswith("SSS"):
            suffix = class_level[-1]
            if suffix in ["A", "B"]:
                return sss_core_subjects + sss_science_core_subjects + sss_pure_science_subjects
            elif suffix == "C":
                return sss_core_subjects + sss_science_core_subjects + sss_technical_subjects
            elif suffix == "D":
                return sss_core_subjects + sss_commercial_subjects
            elif suffix in ["E", "F"]:
                return sss_core_subjects + sss_arts_subjects
            else:
                print(f"Invalid suffix: {suffix}")
                return []
        return []

    current_year = datetime.now().year

    # Iterate through the backdate levels to insert exam history
    for i, class_level in enumerate(backdate_levels):
        exam_year = current_year - i  # Adjust the year based on the level difference
        print(f"Exam year: {exam_year}, Class level: {class_level}")
        # Get the subjects based on the class level and suffix
        subjects = get_subjects(class_level)

        # Insert exam history for all subjects for this class level and year
        for subject in subjects:
            score = assign_score()

            # Get the subject ID (assuming subject names map directly to IDs)
            cursor.execute("SELECT subject_id FROM subject WHERE subject_name = %s", (subject,))
            subject_id_result = cursor.fetchone()
            if subject_id_result:
                subject_id = subject_id_result[0]

                # Add the record to the batch for bulk insertion
                exam_history_batch.append((student_id, subject_id, score, exam_year))

                # To prevent memory issues, insert the batch every 1000 records
                if len(exam_history_batch) >= 1000:
                    cursor.executemany("""
                        INSERT INTO ExamHistory (student_id, subject_id, score, exam_year)
                        VALUES (%s, %s, %s, %s)
                    """, exam_history_batch)
                    exam_history_batch.clear()  # Clear the batch after insertion
                    print(f"Inserted 1000 records...")



try:
    # Establish database connection
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor(buffered=True)  # Use a buffered cursor to ensure results are processed
    print("Database connection established.")

    # Fetch student data and enrollment information for students currently in JSS2 or higher
    query = """
    SELECT s.students_id, s.class_level, s.enrollment_date
    FROM students_data s
    WHERE s.class_level LIKE 'JSS2%'
       OR s.class_level LIKE 'JSS3%'
       OR s.class_level LIKE 'SSS1%'
       OR s.class_level LIKE 'SSS2%'
       OR s.class_level LIKE 'SSS3%'
       limit 100
    """

    cursor.execute(query)
    students = cursor.fetchall()

    exam_history_batch = []

    for count, student in enumerate(students, start=1):
        student_id = student[0]
        class_level = student[1]
        enrollment_date = student[2]

        # Parse the enrollment year
        enrollment_year = int(enrollment_date.strftime('%Y'))

        # Insert exam history starting from the current class level back to JSS1
        insert_exam_history_batch(student_id, enrollment_year, class_level, cursor, exam_history_batch)

        # Print progress for every 100 students processed
        if count % 100 == 0:
            print(f"Processed {count} students...")

    # Insert any remaining records in the batch
    if exam_history_batch:
        cursor.executemany("""
            INSERT INTO ExamHistory (student_id, subject_id, score, exam_year)
            VALUES (%s, %s, %s, %s)
        """, exam_history_batch)
        connection.commit()
        print(f"Inserted final batch of {len(exam_history_batch)} records...")

    # Commit the transaction
    connection.commit()
    print("Exam history data inserted successfully!")

except mysql.connector.Error as err:
    print(f"Error: {err}")
finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection closed.")
