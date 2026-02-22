import psycopg2

## Bu değeri localinde çalışırken kendi passwordün yap. Ama kodu pushlarken 'postgres' olarak bırak.
password = 'postgres'


def connect_db():
    conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="postgres",
    user="postgres",
    password=password)
    return conn


def question_1_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute('select * from students where age >20')
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


def question_2_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute("select * from courses where category='Veritabanı'")
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


def question_3_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute("select * from students where first_name like 'A%'")
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


def question_4_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute("select * from courses where course_name like '%SQL%'")
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


def question_5_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute('select * from  students where age between 22 and 24 ')
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


def question_6_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute('select first_name,last_name from enrollments as e join students as s on s.student_id=e.student_id join courses as c on c.course_id=e.course_id')
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


def question_7_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute("SELECT c.category, COUNT(DISTINCT e.student_id) AS ogrenci_sayisi FROM courses c JOIN enrollments e ON c.course_id = e.course_id WHERE c.category = 'Veritabanı' GROUP BY c.category;")
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


def question_8_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute('select course_name,name from course_instructors as ci join courses as c on ci.course_id=c.course_id join instructors as i on i.instructor_id=ci.instructor_id')
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


def question_9_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute('SELECT * FROM students s LEFT JOIN enrollments e ON s.student_id = e.student_id WHERE e.enrollment_id IS NULL')
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


def question_10_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute('select c.course_name,avg(s.age) from courses as c join enrollments  as e on c.course_id=e.course_id join students as s on s.student_id=e.student_id group by course_name')
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


def question_11_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute('SELECT s.first_name, s.last_name, COUNT(e.enrollment_id) AS toplam_kurs FROM students s LEFT JOIN enrollments e ON s.student_id = e.student_id GROUP BY s.student_id')
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


def question_12_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute('SELECT i.name, COUNT(c.course_id) FROM course_instructors AS c LEFT JOIN instructors i ON c.instructor_id = i.instructor_id GROUP BY i.name having count(c.course_id)>1')
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


def question_13_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute('select c.course_name, count(e.student_id) from enrollments as e join courses as c on e.course_id =c.course_id group by c.course_name')
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


def question_14_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute("SELECT s.first_name, s.last_name FROM students s JOIN enrollments e ON s.student_id = e.student_id JOIN courses c ON e.course_id = c.course_id WHERE c.course_name IN ('SQL Temelleri', 'İleri SQL') GROUP BY s.student_id, s.first_name, s.last_name HAVING COUNT(DISTINCT c.course_name) = 2")
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


def question_15_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute('SELECT s.first_name, s.last_name,c.course_name,i.name,e.enrollment_date FROM enrollments e JOIN students s ON e.student_id = s.student_id JOIN courses c ON e.course_id = c.course_id JOIN course_instructors ci ON c.course_id = ci.course_id JOIN instructors i ON ci.instructor_id = i.instructor_id')
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data