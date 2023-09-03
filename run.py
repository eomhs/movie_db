from mysql.connector import connect
from mysql.connector.errors import IntegrityError
import csv
import numpy as np
import heapq

connection = connect(
    host='your_host',
    port=7000,
    user='your_user',
    password='your_password',
    db='your_db',
    charset='utf8')

cursor = connection.cursor(dictionary=True)

add_movie = ("INSERT INTO movie (`title`, `director`, `price`) "
                "VALUES (%s, %s, %s)")
    
add_customer = ("INSERT INTO customer (`name`, `age`, `class`)"
                "VALUES (%s, %s, %s)")

def initialize_database():

    # Create tables if not exist
    TABLES = {}
    TABLES['movie'] = (
        "CREATE TABLE `movie` ("
        "   `mov_id` int(10) NOT NULL AUTO_INCREMENT PRIMARY KEY,"
        "   `title` varchar(255) UNIQUE,"
        "   `director` varchar(255) NOT NULL,"
        "   `price` int(10) NOT NULL"
        ") ENGINE=InnoDB")
    
    TABLES['customer'] = (
        "CREATE TABLE `customer` ("
        "   `cus_id` int(10) NOT NULL AUTO_INCREMENT PRIMARY KEY,"
        "   `name` varchar(255),"
        "   `age` int(10),"
        "   `class` varchar(10) NOT NULL,"
        "   UNIQUE KEY customer_uk_1 (`name`, `age`)"
        ") ENGINE=InnoDB")
    
    TABLES['reservation'] = (
        "CREATE TABLE `reservation` ("
        "   `mov_id` int(10),"
        "   `cus_id` int(10),"
        "   `reserve_price` int(10) NOT NULL,"
        "   UNIQUE KEY reservation_uk_1 (`mov_id`, `cus_id`),"
        "   CONSTRAINT `reservation_ibfk_1` FOREIGN KEY (`mov_id`)"
        "       REFERENCES `movie` (`mov_id`) ON DELETE CASCADE,"
        "   CONSTRAINT `reservation_ibfk_2` FOREIGN KEY (`cus_id`)"
        "       REFERENCES `customer` (`cus_id`) ON DELETE CASCADE"
        ") ENGINE=InnoDB")
    
    TABLES['rating'] = (
        "CREATE TABLE `rating` ("
        "   `mov_id` int(10),"
        "   `cus_id` int(10),"
        "   `rating` int(2) NOT NULL,"
        "   UNIQUE KEY rating_uk_1 (`mov_id`, `cus_id`),"
        "   CONSTRAINT `rating_ibfk_1` FOREIGN KEY (`mov_id`)"
        "       REFERENCES `movie` (`mov_id`) ON DELETE CASCADE,"
        "   CONSTRAINT `rating_ibfk_2` FOREIGN KEY (`cus_id`)"
        "       REFERENCES `customer` (`cus_id`) ON DELETE CASCADE"
        ") ENGINE=InnoDB")
    
    for table_name in TABLES:
        table_description = TABLES[table_name]
        try:
            cursor.execute(table_description)
        except Exception:
            pass

    # Insert values in data.csv
    f = open('data.csv', 'r')
    rdr = csv.reader(f)
    for title, director, price, name, age, rank in rdr:
        if price == "price":
            continue
        price = int(price)
        age = int(age)
        movie = (title, director, price)
        customer = (name, age, rank)
        isValid = True

        # Insert movie if no exception occurs
        if price < 0 or price > 100000:
            print('Movie price should be from 0 to 100000')
            isValid = False
        else:
            try:
                cursor.execute(add_movie, movie)
            except IntegrityError:
                cursor.execute("ALTER TABLE movie AUTO_INCREMENT = 1")
            except:
                pass

        # Insert customer if no exception occurs
        if age < 12 or age > 110:
            print('User age should be from 12 to 110')
            isValid = False
        elif rank not in ('basic', 'premium', 'vip'):
            print('User class should be basic, premium or vip')
            isValid = False
        else:
            try:
                cursor.execute(add_customer, customer)
            except IntegrityError:
                cursor.execute("ALTER TABLE customer AUTO_INCREMENT = 1")
            except:
                pass

        # Insert reservation if no exception occured
        if isValid:
            cursor.execute("SELECT mov_id FROM movie WHERE title = %s", (title,))
            row = cursor.fetchone()
            mov_id = row["mov_id"]
            cursor.execute("SELECT cus_id FROM customer WHERE name = %s AND age = %s", (name, age))
            row = cursor.fetchone()
            cus_id = row["cus_id"]
            if rank == 'premium':
                price *= 0.75
            elif rank == 'vip':
                price *= 0.5
            try:
                cursor.execute("INSERT INTO reservation VALUES (%s, %s, %s)", (mov_id, cus_id, price))
            except:
                print(f'User {cus_id} already booked movie {mov_id}')
                cursor.execute("ALTER TABLE reservation AUTO_INCREMENT = 1")
    
    connection.commit()
    print('Database successfully initialized')

def reset():
    while(True):
        s = input('Do you want to reset data? y/n: ')
        if s == 'y':
            cursor.execute("DROP TABLE IF EXISTS `reservation`")
            cursor.execute("DROP TABLE IF EXISTS `rating`")
            cursor.execute("DROP TABLE IF EXISTS `movie`")
            cursor.execute("DROP TABLE IF EXISTS `customer`")
            initialize_database(); # After drop all tables, initialize databse
            break
        elif s == 'n':
            break
        else:
            print('Invalid input. Try again')

def print_movies():

    # Get information and print
    cursor.execute("""
    SELECT movie.mov_id, title, director, price, 
       AVG(reserve_price) AS avg_reserve_price,
       COUNT(reservation.mov_id) AS num_reservations,
       AVG(rating) AS avg_rating
        FROM movie
        LEFT JOIN reservation ON movie.mov_id = reservation.mov_id
        LEFT JOIN rating ON reservation.mov_id = rating.mov_id AND reservation.cus_id = rating.cus_id
        GROUP BY movie.mov_id
        ORDER BY movie.mov_id
    """)

    line = '-' * 170
    print(line)
    headers = ['id', 'title', 'director', 'price', 'avg. price', 'reservation', 'avg. rating']
    print("{:9}{:70}{:35}{:8}{:13}{:14}{:9}".format(*headers))
    print(line)

    result = cursor.fetchall()
    for row in result:
        values = [row['mov_id'], row['title'], row['director'], row['price'], row['avg_reserve_price'] if row['avg_reserve_price'] is not None else 'None', 
                  row['num_reservations'], row['avg_rating'] if row['avg_rating'] is not None else 'None']
        print("{:<9}{:70}{:35}{:<8}{:<13}{:<14}{:<9}".format(*values))
    print(line)
    

def print_users():
    cursor.execute("""
    SELECT cus_id, name, age, class
        FROM customer
        ORDER BY cus_id
    """)

    line = '-' * 65
    print(line)
    headers = ['id', 'name', 'age', 'class']
    print("{:9}{:35}{:10}{:10}".format(*headers))
    print(line)

    result = cursor.fetchall()
    for row in result:
        values = [row['cus_id'], row['name'], row['age'], row['class']]
        print("{:<9}{:35}{:<10}{:10}".format(*values))
    print(line)
   

def insert_movie():
    title = input('Movie title: ')
    director = input('Movie director: ')
    price = int(input('Movie price: '))
    movie = (title, director, price)

    if price < 0 or price > 100000:
        print('Movie price should be from 0 to 100000')
        return
    
    try:
        cursor.execute(add_movie, movie)
    except IntegrityError:
        print(f'Movie {title} already exists')
        cursor.execute("ALTER TABLE movie AUTO_INCREMENT = 1")
        return

    # Insert success
    connection.commit()
    print('One movie successfully inserted')


def remove_movie():
    movie_id = input('Movie ID: ')

    delete_movie = ("DELETE FROM movie "
                    "   WHERE mov_id = %s")
    
    cursor.execute(delete_movie, (movie_id,))

    if cursor.rowcount == 0:
        print(f'Movie {movie_id} does not exist')
        return
    
    # Remove success
    connection.commit()
    print('One movie successfully removed')
   

def insert_user():
    name = input('User name: ')
    age = int(input('User age: '))
    rank = input('User class: ')
    customer = (name, age, rank)

    if age < 12 or age > 110:
        print('User age should be from 12 to 110')
        return
    
    if rank not in ('basic', 'premium', 'vip'):
        print('User class should be basic, premium or vip')
        return
    
    try:
        cursor.execute(add_customer, customer)
    except IntegrityError:
        print(f'The user ({name}, {age}) already exists')
        cursor.execute("ALTER TABLE customer AUTO_INCREMENT = 1")
        return

    # Insert success
    connection.commit()
    print('One user successfully inserted')


def remove_user():
    user_id = input('User ID: ')

    delete_customer = ("DELETE FROM customer "
                       "    WHERE cus_id = %s")

    cursor.execute(delete_customer, (user_id,))

    if cursor.rowcount == 0:
        print(f'User {user_id} does not exist')
        return

    connection.commit()
    print('One user successfully removed')


def book_movie():
    movie_id = input('Movie ID: ')
    user_id = input('User ID: ')

    # Check user existence
    if check_existence('customer', 'cus_id', user_id) == False:
        print(f'User {user_id} does not exist')
        return

    # Get class
    rank = get_class(user_id)

    # Get price
    cursor.execute("SELECT price FROM movie WHERE mov_id = %s", (movie_id,))
    prices = cursor.fetchall()
    if len(prices) == 0:
        print(f'Movie {movie_id} does not exist')
        return
    price = prices[0]['price']

    # Adjust price by class
    if rank == 'premium':
        price *= 0.75
    elif rank == 'vip':
        price *= 0.5

    # Check current number of reservations of this movie
    cursor.execute("SELECT * FROM reservation WHERE mov_id = %s", (movie_id,))
    reservations = cursor.fetchall()
    if len(reservations) >= 10:
        print(f'Movie {movie_id} has already been fully booked')
        return
    
    # Try booking
    reservation = (movie_id, user_id, price)
    try:
        cursor.execute("INSERT INTO reservation VALUES (%s, %s, %s)", reservation)
    except IntegrityError:
        print(f'User {user_id} already booked movie {movie_id}')
        cursor.execute("ALTER TABLE reservation AUTO_INCREMENT = 1")
        return

    # Booking success
    connection.commit()
    print('Movie successfully booked')
  

def rate_movie():
    movie_id = input('Movie ID: ')
    user_id = input('User ID: ')
    rating = int(input('Ratings (1~5): '))

    # Check movie existence
    if check_existence('movie', 'mov_id', movie_id) == False:
        print(f'Movie {movie_id} does not exist')
        return
    
    # Check user existence
    if check_existence('customer', 'cus_id', user_id) == False:
        print(f'User {user_id} does not exist')
        return
    
    # Check rating range correctness
    if rating < 1 or rating > 5:
        print(f'Wrong value for a rating')
        return
    
    # Check reservation history
    cursor.execute("SELECT * from reservation WHERE mov_id = %s AND cus_id = %s", (movie_id, user_id))
    reservations = cursor.fetchall()
    if len(reservations) == 0:
        print(f'User {user_id} has not booked movie {movie_id} yet')
        return
    
    # Try rating
    rating_record = (movie_id, user_id, rating)
    try:
        cursor.execute("INSERT INTO rating VALUES (%s, %s, %s)", rating_record)
    except IntegrityError:
        print(f'User {user_id} has already rated movie {movie_id}')
        cursor.execute("ALTER TABLE rating AUTO_INCREMENT = 1")
        return

    # Rating success
    connection.commit()
    print('Movie successfully rated')


def print_users_for_movie():
    movie_id = input('Movie ID: ')

    # Check movie existence
    if check_existence('movie', 'mov_id', movie_id) == False:
        print(f'Movie {movie_id} does not exist')
        return
    
    # Get user information and print
    cursor.execute("""
    SELECT customer.cus_id, name, age, reserve_price, rating 
        FROM customer NATURAL JOIN reservation 
        LEFT JOIN rating ON reservation.mov_id = rating.mov_id AND reservation.cus_id = rating.cus_id
        WHERE reservation.mov_id = %s 
        ORDER BY customer.cus_id
    """, (movie_id,))

    line = '-' * 80
    print(line)
    headers = ['id', 'name', 'age', 'res. price', 'rating']
    print("{:<9}{:35}{:<10}{:<13}{:<9}".format(*headers))
    print(line)

    result = cursor.fetchall()
    for row in result:
        values = [row['cus_id'], row['name'], row['age'], row['reserve_price'], 
                  row['rating'] if row['rating'] is not None else 'None']
        print("{:<9}{:35}{:<10}{:<13}{:<9}".format(*values))
    print(line)


def print_movies_for_user():
    user_id = input('User ID: ')

    # Check user existence
    if check_existence('customer', 'cus_id', user_id) == False:
        print(f'User {user_id} does not exist')
        return

    # Get movie information and print
    cursor.execute("""
    SELECT movie.mov_id, title, director, reserve_price, rating
        FROM movie NATURAL JOIN reservation 
        LEFT JOIN rating ON reservation.mov_id = rating.mov_id AND reservation.cus_id = rating.cus_id
        WHERE reservation.cus_id = %s
    """, (user_id,))
    
    line = '-' * 140
    print(line)
    headers = ['id', 'title', 'director', 'res. price', 'rating']
    print("{:<9}{:70}{:<35}{:<13}{:<9}".format(*headers))
    print(line)

    result = cursor.fetchall()
    for row in result:
        values = [row['mov_id'], row['title'], row['director'], row['reserve_price'],
                  row['rating'] if row['rating'] is not None else 'None']
        print("{:<9}{:70}{:<35}{:<13}{:<9}".format(*values))
    print(line)


def recommend_popularity():
    user_id = input('User ID: ')

    # Check user existence
    if check_existence('customer', 'cus_id', user_id) == False:
        print(f'User {user_id} does not exist')
        return

    # Get information
    cursor.execute(f"""
    SELECT movie.mov_id, title, director, price,
        COUNT(reservation.mov_id) AS num_reservations,
        AVG(rating) AS avg_rating
        FROM movie
        LEFT JOIN reservation ON movie.mov_id = reservation.mov_id
        LEFT JOIN rating ON reservation.mov_id = rating.mov_id AND reservation.cus_id = rating.cus_id
        GROUP BY movie.mov_id
        ORDER BY movie.mov_id
    """)
    result = cursor.fetchall()
    rank = get_class(user_id)

    # Iterate and find rating-based and popularity-based item
    highest_rating, highest_popularity = 0, -1
    rating_item, popularity_item = [], []
    for row in result:
        id = row['mov_id']
        title = row['title']
        director = row['director']
        price = row['price']
        reservation = row['num_reservations']
        rating = row['avg_rating']
        if not rating_item and rating is None and not check_reservation(id, user_id):
            rating_item = [id, title, director, price, reservation, 'None']
        if rating is not None and rating > highest_rating and not check_reservation(id, user_id):
            rating_item = [id, title, director, price, reservation, rating]
            highest_rating = rating
        if reservation > highest_popularity and not check_reservation(id, user_id):
            popularity_item = [id, title, director, price, reservation, rating]
            highest_popularity = reservation
    if popularity_item[5] is None:
        popularity_item[5] = 'None'
        
    # Change price to reserve price
    if rank == 'premium':
        rating_item[3] *= 0.75
        popularity_item[3] *= 0.75
    elif rank == 'vip':
        rating_item[3] *= 0.5
        popularity_item[3] *= 0.5

    # Print rating-based item
    line = '-' * 150
    print(line)
    print('Rating-based')
    headers = ['id', 'title', 'director', 'res. price', 'reservation', 'avg. rating']
    print("{:<9}{:70}{:<35}{:<13}{:<13}{:<13}".format(*headers))
    print(line)
    print("{:<9}{:70}{:<35}{:<13}{:<13}{:<13}".format(*rating_item))

    # Print popularity-based item
    print(line)
    print('Popularity-based')
    print("{:<9}{:70}{:<35}{:<13}{:<13}{:<13}".format(*headers))
    print(line)
    print("{:<9}{:70}{:<35}{:<13}{:<13}{:<13}".format(*popularity_item))
    print(line)
  

def recommend_item_based():
    user_id = int(input('User ID: '))
    N = int(input('Item number: '))

    # Check user existence
    if check_existence('customer', 'cus_id', user_id) == False:
        print(f'User {user_id} does not exist')
        return

    # Check rating existence
    cursor.execute("SELECT * FROM rating WHERE cus_id = %s", (user_id,))
    results = cursor.fetchall()
    if len(results) == 0:
        print('Rating does not exist')
        return

    # Get n, m and make user-item matrix
    cursor.execute("SELECT cus_id FROM customer ORDER BY cus_id")
    result = cursor.fetchall()
    users = []
    for row in result:
        users.append(row['cus_id'])
    n = cursor.rowcount

    cursor.execute("SELECT mov_id FROM movie ORDER BY mov_id")
    result = cursor.fetchall()
    items = []
    for row in result:
        items.append(row['mov_id'])
    m = cursor.rowcount

    arr_ui = np.zeros((n, m))

    cursor.execute("SELECT * FROM rating")
    ratings = cursor.fetchall()
    for row in ratings:
        r = users.index(row['cus_id'])
        c = items.index(row['mov_id'])
        rating = row['rating']
        arr_ui[r, c] = rating

    column_avg = np.where(np.all(arr_ui == 0, axis=0), 0, np.mean(arr_ui[arr_ui != 0], axis=0))
    not_rated = np.where(np.isin(arr_ui[users.index(user_id)], [0]))[0]
    zero_indices = np.where(arr_ui == 0)
    arr_ui[zero_indices] = column_avg[zero_indices[1]]

    # Get average and make similarity matrix
    avg = np.mean(arr_ui)
    arr_s = np.zeros((m, m))

    for i in range(m):
        arr_s[i, i] = 1

    for i in range(m):
        for j in range(i + 1):
            numerator = 0
            for k in range(n):
                numerator += (arr_ui[k, i] - avg) * (arr_ui[k, j] - avg)
            denominator1 = 0
            denominator2 = 0
            for k in range(n):
                denominator1 += (arr_ui[k, i] - avg) ** 2
                denominator2 += (arr_ui[k, j] - avg) ** 2
            denominator1 = denominator1 ** 0.5
            denominator2 = denominator2 ** 0.5
            similarity = numerator / (denominator1 * denominator2)
            arr_s[i, j] = similarity
            arr_s[j, i] = similarity
    
    # Caculate weighted sum
    heap = []
    for c in not_rated:
        numerator = 0
        denominator = 0
        for i in range(m):
            if i == c:
                continue
            else:
                numerator += arr_s[c, i] * arr_ui[users.index(user_id), i]
                denominator += arr_s[c, i]
        expected_rating = numerator / denominator
        heapq.heappush(heap, (-expected_rating, items[c]))

    # Find N (or less) recommendable movies
    movies = []
    while N and heap:
        expected_rating_minus, movie_id = heapq.heappop(heap)
        expected_rating = -expected_rating_minus
        if check_reservation(movie_id, user_id):
            continue
        else:
            cursor.execute(f"""
            SELECT title, director, price, AVG(rating) AS avg_rating
                FROM movie LEFT JOIN rating ON movie.mov_id = rating.mov_id
                WHERE movie.mov_id = {movie_id}
            """)
            row = cursor.fetchone()

            movie = []
            movie.append(movie_id)
            movie.append(row['title'])
            movie.append(row['director'])
            price = int(row['price'])
            rank = get_class(user_id)
            if rank == 'premium':
                price *= 0.75
            elif rank == 'vip':
                price *= 0.5
            movie.append(price)
            avg_rating = row['avg_rating'] if row['avg_rating'] is not None else 'None'
            movie.append(avg_rating)
            movie.append(expected_rating)

            movies.append(movie)
            N -= 1

    # Print recommendable movies
    line = '-' * 150
    print(line)
    headers = ['id', 'title', 'director', 'res. price', 'avg. rating', 'expected rating']
    print("{:<9}{:70}{:<35}{:<13}{:<13}{:<13}".format(*headers))
    print(line)
    for values in movies:
        print("{:<9}{:70}{:<35}{:<13}{:<13}{:<13}".format(*values))
    print(line)


# Existence checking function
def check_existence(table, attr, value):
    cursor.execute(f"SELECT * FROM {table} WHERE {attr} = {value}")
    results = cursor.fetchall()
    if len(results) == 0:
        return False
    else:
        return True
    

# Reservation checking function
def check_reservation(movie_id, user_id):
    cursor.execute(f"SELECT * from reservation WHERE mov_id = {movie_id} AND cus_id = {user_id}")
    results = cursor.fetchall()
    if len(results) == 0:
        return False
    else:
        return True


# Class getting function
def get_class(user_id):
    cursor.execute("SELECT class FROM customer WHERE cus_id = %s", (user_id,))
    row = cursor.fetchone()
    rank = row['class']
    return rank


def main():
    while True:
        print('============================================================')
        print('1. initialize database')
        print('2. print all movies')
        print('3. print all users')
        print('4. insert a new movie')
        print('5. remove a movie')
        print('6. insert a new user')
        print('7. remove an user')
        print('8. book a movie')
        print('9. rate a movie')
        print('10. print all users who booked for a movie')
        print('11. print all movies rated by an user')
        print('12. recommend a movie for a user using popularity-based method')
        print('13. recommend a movie for a user using item-based collaborative filtering')
        print('14. exit')
        print('15. reset database')
        print('============================================================')
        menu = int(input('Select your action: '))

        if menu == 1:
            initialize_database()
        elif menu == 2:
            print_movies()
        elif menu == 3:
            print_users()
        elif menu == 4:
            insert_movie()
        elif menu == 5:
            remove_movie()
        elif menu == 6:
            insert_user()
        elif menu == 7:
            remove_user()
        elif menu == 8:
            book_movie()
        elif menu == 9:
            rate_movie()
        elif menu == 10:
            print_users_for_movie()
        elif menu == 11:
            print_movies_for_user()
        elif menu == 12:
            recommend_popularity()
        elif menu == 13:
            recommend_item_based()
        elif menu == 14:
            print('Bye!')
            break
        elif menu == 15:
            reset()
        else:
            print('Invalid action')


if __name__ == "__main__":
    main()