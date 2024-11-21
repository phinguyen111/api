from database_connection import Neo4jConnection
import requests
import time
import os
import asyncio

uri = os.getenv("NEO4J_URI", "neo4j+s://aadff3f9.databases.neo4j.io")
username = os.getenv("NEO4J_USER", "neo4j")
password = os.getenv("NEO4J_PASSWORD", "TVuvrmUqxBe3u-gDv6oISHDlZKLxUJKz3q8FrOXyWmo")

neo4j_connection = Neo4jConnection(uri, username, password)

async def fetch_and_save_transactions(address, limit=25):
    etherscan_api_key = os.getenv("ETHERSCAN_API_KEY", "IGVQMMEFYD8K2DK22ZTFV6WK1RH8KP98IS")
    url = f"https://api.etherscan.io/api?module=account&action=txlist&address={address}&startblock=0&endblock=99999999&sort=desc&apikey={etherscan_api_key}"

    try:
        start_time = time.time()  # Bắt đầu theo dõi thời gian
        print(f"Fetching transactions for address {address}")
        response = requests.get(url)
        data = response.json()

        if data["status"] == "1" and len(data["result"]) > 0:
            transactions = data["result"][:limit]  # Fetch limit transactions
            print(f"Fetched {len(transactions)} transactions in {time.time() - start_time:.2f} seconds")
            neo4j_start_time = time.time()  # Thời gian bắt đầu lưu vào Neo4j
            
            for tx in transactions:
                if int(tx['value']) > 0:
                    # Convert wei to ether
                    amount_in_ether = float(int(tx['value']) / (10 ** 18))
                    
                    # Save to Neo4j
                    query = f"""
                    MERGE (s:Address {{address: '{tx['from']}'}})
                    MERGE (r:Address {{address: '{tx['to']}'}})
                    MERGE (t:Transaction {{
                        id: '{tx['hash']}',
                        amount: {amount_in_ether},
                        timestamp: {tx['timeStamp']}
                    }})
                    MERGE (s)-[:SENT_TO {{amount: {amount_in_ether}, timestamp: {tx['timeStamp']}}}]->(t)
                    MERGE (t)-[:RECEIVED_FROM {{amount: {amount_in_ether}, timestamp: {tx['timeStamp']}}}]->(r)
                    """
                    neo4j_connection.execute_query(query)
                    print(f"Saved transaction {tx['hash']} to Neo4j")
                    
                    # Chờ 0.2 giây giữa các lần xử lý để tuân thủ giới hạn 5 API calls/sec
                    await asyncio.sleep(0.2)
            
            print(f"Saved all transactions to Neo4j in {time.time() - neo4j_start_time:.2f} seconds")
            return transactions
        else:
            print(f"No transactions found for address {address}")
            return []
    except Exception as e:
        print(f"Error fetching transactions: {e}")
        return []

# Hàm gọi async
async def main():
    address = "0xYourAddressHere"
    await fetch_and_save_transactions(address)

# Chạy hàm bất đồng bộ
if __name__ == "__main__":
    asyncio.run(main())
