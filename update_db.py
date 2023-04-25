import mysql.connector
from update_lib import *

def main():
    # If the database does not exist, create it
    create_database()

    mydb = mysql.connector.connect(
      host="localhost",
      user="dereek69",
      password="352846",
      database="price_tvl_correlation"
    )

    mycursor = mydb.cursor()

    # Prompt the user a multiple choice menu to select one of three options:
    # 1. Update the token list
    # 2. Update the historical tvl and price data for all protocols
    # 3. Delete the database and start over

    print("Select an option:")
    print("1. Update the token list")
    print("2. Update the historical tvl and price data for all protocols")
    print("3. Delete the database and start over")
    print("4. Exit")

    option = input()

    if option == "1":
        create_and_update_token_table(mycursor)
        mydb.commit()
    elif option == "2":
        update_historical_data(mydb, mycursor)
    elif option == "3":
        delete_database(mycursor)
        mydb.commit()
    elif option == "4":
        exit()
    else:
        print("Invalid option")


if __name__ == "__main__":
    main()