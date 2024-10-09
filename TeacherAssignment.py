import mysql.connector
import random

# MySQL database connection configuration
db_config = {
    'host': 'host_name',
    'user': 'user',
    'password': 'password',
    'database': 'databaseName',
    'port': 'port'
}

try:
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()

    # Fetch all teachers ordered by experience
    cursor.execute("SELECT teacher_id, experience FROM Teachers ORDER BY experience DESC")
    teachers = cursor.fetchall()

    # Group teachers by experience levels
    teachers_by_experience = {
        '10+': [teacher for teacher in teachers if '10+' in teacher[1]],
        '6-10': [teacher for teacher in teachers if '6-10' in teacher[1]],
        '1-5': [teacher for teacher in teachers if '1-5' in teacher[1]],
    }


    # Fetch all subjects and distinct subject names
    cursor.execute("SELECT subject_id, subject_name, class_name FROM subject")
    subjects = cursor.fetchall()

    cursor.execute("SELECT DISTINCT subject_name FROM subject")
    distinct_subjects = [row[0] for row in cursor.fetchall()]

    # Fetch room IDs and room names from the classroom table
    cursor.execute("SELECT room_id, room_name FROM classroom")
    classrooms = cursor.fetchall()
    classroom_map = {room_name.lower(): room_id for room_id, room_name in classrooms}

    teacher_assignment_records = []  # To collect batch inserts

    # Dictionary to track how many teachers each subject needs based on their frequency
    subject_teacher_count = {
        "English Language": 6,
        "Mathematics": 6,
        "Civic Education": 6,
        "Agricultural Science": 5,
        "French": 4,
        "Physical and Health Education": 3,
        "Basic Technology": 3,
        "Music": 3,
        "Home Economics": 3,
        "Computer Studies": 3,
        "Business Studies": 3,
        "Christian Religion Studies": 3,
        "Fine Arts/Creative Art": 3,
        "Social Studies": 3,
        "Basic Science": 3,
        "Yoruba": 3,
        "Physics": 2,
        "Computer Science": 2,
        "Marketing": 2,
        "Biology": 2,
        "Chemistry": 2,
        "Economics": 1,
        "Government": 1,
        "Literature – in- English": 1,
        "Christian Religion Knowledge": 1,
        "Painting an Decoration": 1,
        "Geography": 1,
        "Financial Account": 1,
        "Book Keeping": 1,
        "Typewriting": 1,
        "Office Practice": 1,
        "Commerce": 1,
        "Data Processing": 1
    }

    # Assign teachers to each subject based on its count and teacher experience
    for subject_name, needed_teachers in subject_teacher_count.items():
        # Get all instances of the subject (class-specific) from the database
        subject_instances = [(sub_id, sub_name, class_name) for sub_id, sub_name, class_name in subjects if sub_name == subject_name]
        total_instances = len(subject_instances)

        # Shuffle the list of teachers in each experience group to randomize assignments
        for exp_group in teachers_by_experience.values():
            random.shuffle(exp_group)

        teacher_allocations = []

        # Determine how to split the instances across experience groups
        exp_distribution = {
            '10+': min(7, total_instances // 2),  # Highly experienced teachers handle up to 7 instances
            '6-10': min(5, total_instances // 3),  # Mid-experience teachers handle up to 5 instances
            '1-5': total_instances - (min(7, total_instances // 2) + min(5, total_instances // 3))  # Less experienced teachers take the rest
        }

        instance_index = 0

        # Assign teachers based on the calculated distribution
        for exp_level, teacher_group in teachers_by_experience.items():
            group_count = exp_distribution[exp_level]

            for _ in range(group_count):
                if instance_index >= total_instances:
                    break

                teacher = teacher_group[instance_index % len(teacher_group)]
                teacher_id = teacher[0]
                subject_id, _, class_name = subject_instances[instance_index]

                # Get the corresponding room ID for the class
                room_id = classroom_map.get(class_name.lower())
                if room_id is None:
                    print(f"Warning: No matching room found for class_name: {class_name}")
                    continue

                # Set start_date and end_date
                start_date = "2024-09-16"
                end_date = "2024-11-25"

                # Collect the assignment
                teacher_assignment_records.append((teacher_id, subject_id, room_id, start_date, end_date))

                instance_index += 1

    # Insert teacher assignments in batch
    insert_query = """
    INSERT INTO TeacherAssignment (teacher_id, subject_id, room_id, start_date, end_date)
    VALUES (%s, %s, %s, %s, %s)
    """
    cursor.executemany(insert_query, teacher_assignment_records)

    # Commit the transaction
    connection.commit()
    print("Teacher assignments inserted successfully!")

except mysql.connector.Error as err:
    print(f"Error: {err}")
finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection closed.")
