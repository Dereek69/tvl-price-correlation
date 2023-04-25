import mysql.connector
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

mydb = mysql.connector.connect(
    host="localhost",
    user="lol",
    password="xd",
    database="price-tvl-correlation"
    )

mycursor = mydb.cursor()

# Ask the user what the time period should be in months
print("How many months should the time period be?")
months = input()

# Ask the user what the minimum tvl should be in Millions of USD
print("What should the minimum tvl be in Millions of USD?")
min_tvl = input()

# Fetch all the protocols from where the tvl was greater than 0 at x months ago and where the last tvl recorded is greater than the minimum tvl
mycursor.execute("SELECT * FROM tokens WHERE defillama_slug IN (SELECT defillama_slug FROM (SELECT defillama_slug, SUM(tvl) AS tvl FROM (SELECT * FROM (SELECT * FROM protocols WHERE date > DATE_SUB(NOW(), INTERVAL " + months + " MONTH)) AS protocols JOIN (SELECT * FROM tokens WHERE defillama_slug IS NOT NULL) AS tokens ON protocols.slug = tokens.defillama_slug) AS protocols GROUP BY defillama_slug) AS protocols WHERE tvl > " + min_tvl + ")")
protocols = mycursor.fetchall()

# For each protocol, fetch the historical tvl and price data for the last x months.
# Calculate the ratio between the tvl and price for each day and store it in a list
# Calculate the 20 protocols with the highest ratio and the 20 protocols with the lowest ratio
# Plot the ratio for each protocol
for protocol in protocols:
    mycursor.execute("SELECT * FROM " + protocol[3] + " WHERE date > DATE_SUB(NOW(), INTERVAL " + months + " MONTH)")
    data = mycursor.fetchall()

    ratio = []
    for row in data:
        ratio.append(row[2]/row[1])

    ratio = pd.Series(ratio)
    ratio = ratio.replace([np.inf, -np.inf], np.nan)
    ratio = ratio.dropna()
    ratio = ratio.sort_values()
    ratio = ratio.reset_index(drop=True)

    plt.plot(ratio)
    plt.title(protocol[0])
    plt.xlabel("Days")
    plt.ylabel("Ratio")
    plt.show()
    
  
