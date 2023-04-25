import mysql.connector, requests, time

# Get the list of all the TVLs from the DefiLlama API
def get_all_protocols():
    url = "https://api.llama.fi/protocols"
    response = requests.request("GET", url)
    return response.json()

# Get the total TVL from the DefiLlama API
def get_total_tvl():
    url = "https://api.llama.fi/charts/tvl"
    response = requests.request("GET", url)
    return response.json()

# Get the historic TVL for a specific protocol from the DefiLlama API
def get_tvl_by_protocol(protocol):
    url = "https://api.llama.fi/protocol/" + protocol
    response = requests.request("GET", url)
    return response.json()['tvl']

# Get the list of all the protocols from the coingecko API
def get_coingecko_protocols():
    url = "https://api.coingecko.com/api/v3/coins/list"
    response = requests.request("GET", url)
    return response.json()

# Finds the similarity between 2 strings. It returns a float between 0 and 1, where 0 is not similar at all and 1 is identical.
def similar(a, b):
    # The similar function, gets 2 strings as inputs and checks how similar they are. It returns a float between 0 and 1, where 0 is not similar at all and 1 is identical.
    # It uses the Levenshtein distance algorithm to calculate the similarity.
    # The algorithm is not case sensitive, so it normalizes the strings to lowercase.
    # It also removes all likely symbols from the strings, like brackets, commas, dots, etc.
    a = a.lower().replace('(', '').replace(')', '').replace(',', '').replace('.', '').replace(' ', '')
    b = b.lower().replace('(', '').replace(')', '').replace(',', '').replace('.', '').replace(' ', '')
    return 1 - (levenshtein(a, b) / max(len(a), len(b)))

# The levenshtein function, gets 2 strings as inputs and calculates the Levenshtein distance between them.
def levenshtein(a, b):
    if not a: return len(b)
    if not b: return len(a)
    return min(levenshtein(a[1:], b[1:])+(a[0] != b[0]), levenshtein(a[1:], b)+1, levenshtein(a, b[1:])+1)

# Gets the historical price data for a token from the coingecko api with a 1 day interval given the coingecko id
def get_price_by_protocol(gecko_id):
    # Add a try except block to catch the error when the coingecko api runs out of requests
    # If the api runs out of requests, the code will wait 5 seconds and try again
    try:
        url = "https://api.coingecko.com/api/v3/coins/" + gecko_id + "/market_chart?vs_currency=usd&days=max&interval=daily"
        response = requests.request("GET", url)
        return response.json()['prices']
    except:
        print("Waiting 5 seconds for the coingecko api to refresh")
        time.sleep(5)
        return get_price_by_protocol(gecko_id)

# Calls the get all protocols function from tvl_pull.py and inserts the data into the tokens table
def update_tokens_table(cursor):
    defillama_protocols = get_all_protocols()
    coingecko_protocols = get_coingecko_protocols()

    for protocol in defillama_protocols:
        #Before inserting the protocol into the database, check if the protocol has a symbol
        if protocol['symbol'] != None and protocol['symbol'] != "-":
            #Print the name of the protocol to the console
            print("Adding protocol: ", protocol['name'])

            # Check if the protocol is not in the database already. And if it isnt, insert it into the database
            cursor.execute("SELECT * FROM tokens WHERE defillama_slug = %s", (protocol['slug'],))
            if cursor.fetchone() == None:
                cursor.execute("INSERT INTO tokens (name, symbol, address, defillama_slug) VALUES (%s, %s, %s, %s)", (protocol['name'], protocol['symbol'], protocol['address'], protocol['slug']))

            # If the defillama api does not provide a gecko_id, try to find the protocol in the coingecko api.
            # To do that, we first need to get all the protocols with the same symbols between the two apis.
            # Then iterate over them and follow these 3 rules for the conflict resolution:
            # 1. If the similarity of the name is more than 80% and the symbol are identical, then it should automatically update the coingecko id.
            # 2. If the symbol is identical and the similarity of the name is less than 80%, then it should propmt the user by printing the names from both apis and ask if the match is correct.
            # 3. If none of the symbols are identical then it should inform the user that no match was found
            
            if protocol['gecko_id'] == None or protocol['gecko_id'] not in [protocol['id'] for protocol in coingecko_protocols]:
                # Get all the protocols with the same symbol between the two apis
                similar_protocols = []
                for coingecko_protocol in coingecko_protocols:
                    if coingecko_protocol['symbol'] == protocol['symbol']:
                        similar_protocols.append(coingecko_protocol)

                # Iterate over the similar protocols and follow the 3 rules for conflict resolution
                for similar_protocol in similar_protocols:
                    print("Checking " + similar_protocol['name'] + " for " + protocol['name'])
                    # Rule 1
                    if similar(similar_protocol['name'], protocol['name']) > 0.8:
                        print("Updating coingecko id for " + protocol['name'] + " from " + protocol['gecko_id'] + " to " + similar_protocol['id'])
                        cursor.execute("UPDATE tokens SET coingecko_id = %s WHERE defillama_slug = %s", (similar_protocol['id'], protocol['slug']))
                        break
                    # Rule 2
                    elif similar(similar_protocol['name'], protocol['name']) < 0.8 and similar_protocol['symbol'] == protocol['symbol']:
                        print("Possible match found for " + protocol['name'] + " and " + similar_protocol['name'])
                        print("Is this correct? (y/n)")
                        answer = input()
                        if answer == "y":
                            print("Updating coingecko id for " + protocol['name'] + " from " + protocol['gecko_id'] + " to " + similar_protocol['id'])
                            cursor.execute("UPDATE tokens SET coingecko_id = %s WHERE defillama_slug = %s", (similar_protocol['id'], protocol['slug']))
                            break
                    # Rule 3
                    else:
                        print("No match found for " + protocol['name'])
                        break
            else:
                # Add the coingecko id from the defillama api to the tokens table
                cursor.execute("UPDATE tokens SET coingecko_id = %s WHERE defillama_slug = %s", (protocol['gecko_id'], protocol['slug']))

# Gets the historical tvl data for a protocol from the defillama api given the protocol slug
def update_historical_tvl(cursor, slug):
            
    #create a new table with the historical tvl and price data for the protocol and link it to the defillama_slug column in the tokens table
    #The protocol slug user for the table name may include the - character. Make sure that the table name is valid
    cursor.execute("CREATE TABLE IF NOT EXISTS `" + slug + "` (date DATE NOT NULL PRIMARY KEY, tvl FLOAT, price FLOAT)")
    
    #get the historical tvl data for the protocol
    tvl_data = get_tvl_by_protocol(slug)

    #insert the historical tvl data into the database
    #the date should be converted from a unix timestamp to a mysql compatible date format
    for data in tvl_data:
        cursor.execute("INSERT IGNORE INTO `" + slug + "` (date, tvl) VALUES (FROM_UNIXTIME(%s), %s)", (data['date'], data['totalLiquidityUSD']))

# Gets the historical price data for a protocol from the coingecko api given the protocol slug
# If the protocol does not have a coingecko id, then it will not be able to get the historical price data
def update_historical_price(cursor, slug):

    #get the updated gecko_id from the database and assign it to a variable
    cursor.execute("SELECT coingecko_id FROM tokens WHERE defillama_slug = %s", (slug,))
    gecko_id = cursor.fetchone()[0]
    
    if gecko_id != None:
        #get the historical price data for the protocol
        price_data = get_price_by_protocol(gecko_id)
    
        #update the table with the historical price data at the same time as the tvl data
        #the date should be converted from a unix timestamp to a mysql compatible date format
        #the price_data is structured as a list of lists, where each list contains a unix timestamp and a price
        for data in price_data:
            cursor.execute("UPDATE `" + slug + "` SET price = %s WHERE date = FROM_UNIXTIME(%s)", (data[1], data[0]))

# Deletes the database
def delete_database(cursor):
    cursor.execute("DROP DATABASE price_tvl_correlation")

# Creates the database
def create_database():
    mydb = mysql.connector.connect(
      host="localhost",
      user="dereek69",
      password="352846"
    )

    mycursor = mydb.cursor()

    mycursor.execute("CREATE DATABASE IF NOT EXISTS price_tvl_correlation")

# Creates the table for the token list and updates it with the latest data from the defillama api
def create_and_update_token_table(cursor):
    # If the tables do not exist, create them
    # Table for the token list. It must include the token name, symbol, token address, defillama slug and coingecko id.
    # The defillama slug should be unique, since it is used to create the table for the historical tvl and price data.
    cursor.execute("CREATE TABLE IF NOT EXISTS tokens (name VARCHAR(255), symbol VARCHAR(255), address VARCHAR(255), defillama_slug VARCHAR(255) PRIMARY KEY, coingecko_id VARCHAR(255))")

    update_tokens_table(cursor)

# Creates or updates the historical tvl and price data and asks the user which option they want to use
def update_historical_data(mydb, mycursor):
    #Prompt the user a multiple choice menu to select one of three options:
    #1. Update the historical tvl and price data for all protocols
    #2. Update the historical tvl and price data for a specific protocol
    #3. Only update the historical price data of the empty protocols

    #get all the slugs from the tokens table
    mycursor.execute("SELECT defillama_slug FROM tokens")
    slugs = mycursor.fetchall()

    #update the historical tvl and price data for each protocol
    for slug in slugs:
        print("Getting historical tvl and price data for " + slug[0])
        update_historical_tvl(mycursor, slug[0])
        update_historical_price(mycursor, slug[0])
        mydb.commit()

