import psycopg2
from data import students_list, course_list


def create_db():
    with psycopg2.connect(database='psql', user='psql', password='12345', host='pg.codecontrol.ru', port=59432) as conn:
        tables = {'course': "CREATE TABLE course(id integer PRIMARY KEY NOT NULL, name varchar(100) NOT NULL);",
                  'student': "CREATE TABLE student(id integer PRIMARY KEY NOT NULL, name varchar(100) NOT NULL, "
                             "gpa numeric(10, 2), birth timestamp with time zone);",
                  'student_course': "CREATE TABLE student_course(course_id integer PRIMARY KEY NOT NULL, "
                                    "student_id integer NOT NULL);"
                  }
        with conn.cursor() as curs:
            for name, query in tables.items():
                curs.execute("select exists(select * from information_schema.tables where table_name='%s');" % name)
                result = curs.fetchone()[0]
                if not result:
                    curs.execute("%s" % query)


def add_course(course_list):
    with psycopg2.connect(database='psql', user='psql', password='12345', host='pg.codecontrol.ru', port=59432) as conn:
        with conn.cursor() as curs:
            for item in course_list:
                for dict_key, value in item.items():
                    curs.execute("select id from course where id=%s" % dict_key)
                    result_query = curs.fetchone()
                    if result_query is None:
                        curs.execute('insert into course(id, name) values(%s, %s)', (dict_key, value))


def get_students(course_id):
    with psycopg2.connect(database='psql', user='psql', password='12345', host='pg.codecontrol.ru', port=59432) as conn:
        with conn.cursor() as curs:
            curs.execute("select student.name, course.name from student join student_course on "
                         "student.id = student_course.student_id join course on "
                         "course.id = student_course.course_id where course_id=%s", (course_id,))
            data = curs.fetchall()
            if data:
                for row in data:
                    print(f"На курсе '{row[1]}' обучается {row[0]}")
            else:
                print(f"На данном курсе никто не обучается!")


def add_students(course_id, students):
    with psycopg2.connect(database='psql', user='psql', password='12345', host='pg.codecontrol.ru', port=59432) as conn:
        with conn.cursor() as curs:
            for item in students:
                for dict_key in item.keys():
                    curs.execute("select id from student where id=%s" % item[dict_key]['id'])
                    result_query = curs.fetchone()
                    if result_query is None:
                        curs.execute('insert into student(id, name, gpa, birth) values(%s, %s, %s, %s)',
                                     (item[dict_key]['id'], item[dict_key]['name'], item[dict_key]['gpa'],
                                      item[dict_key]['birth']))
                    else:
                        print(f"Студент {item[dict_key]['name']} уже есть в базе")
                    curs.execute('select * from student_course where course_id=%s and student_id=%s;',
                                 (course_id, item[dict_key]['id']))
                    if not curs.fetchone():
                        curs.execute('insert into student_course(course_id, student_id) values(%s, %s)',
                                     (course_id, item[dict_key]['id']))
                    else:
                        print(f"Студент {item[dict_key]['name']} уже записан на курс")


def add_student(student):
    with psycopg2.connect(database='psql', user='psql', password='12345', host='pg.codecontrol.ru', port=59432) as conn:
        with conn.cursor() as curs:
            for item in student:
                for dict_key in item.keys():
                    curs.execute("select id from student where id=%s" % item[dict_key]['id'])
                    result_query = curs.fetchone()
                    if result_query is None:
                        curs.execute('insert into student(id, name, gpa, birth) values(%s, %s, %s, %s)',
                                     (item[dict_key]['id'], item[dict_key]['name'], item[dict_key]['gpa'],
                                      item[dict_key]['birth']))
                    else:
                        print(f"Студент {item[dict_key]['name']} уже есть в базе")


def get_student(student_id):
    with psycopg2.connect(database='psql', user='psql', password='12345', host='pg.codecontrol.ru', port=59432) as conn:
        with conn.cursor() as curs:
            curs.execute("select student.name, student.gpa, student.birth, course.name from student join "
                         "student_course on student.id=student_course.student_id join course on "
                         "course.id=student_course.course_id where student_course.student_id=%s", (student_id,))
            result_query = curs.fetchall()
            if len(result_query) == 1:
                print('Имя:', result_query[0][0])
                print('Средняя оценка:', result_query[0][1])
                print('Дата рождения:', result_query[0][2])
                print('Курсы студента:', result_query[0][3])
            elif len(result_query) > 1:
                print('Имя:', result_query[0][0])
                print('Средняя оценка:', result_query[0][1])
                print('Дата рождения:', result_query[0][2])
                courses = ""
                for data in result_query:
                    courses += data[3] + ","
                print("Курсы студента:", courses.rstrip(","))
            else:
                print("Такого студента нет!")

            return curs.fetchall()


if __name__ == "__main__":
    create_db()

    add_student(students_list)

    add_students(2, students_list)

    add_course(course_list)

    get_student(7)

    get_students(2)
