import random
import time
import logging
from faker import Faker

# Initialize faker and logger
fake = Faker()
logging.basicConfig(
    filename="user_activity.log",
    level=logging.INFO,
    format="%(asctime)s - %(message)s"
)

# Categories and regions
categories = ["Adventure", "Relaxation", "Cultural", "Nature", "Historical"]
regions = ["North", "South", "East", "West", "Central"]

# Simulating actions for 50 users
def simulate_user_actions(user_id):
    category = random.choice(categories)
    region = random.choice(regions)

    # Step 1: Browse category-specific group listings
    logging.info(f"User {user_id} is browsing groups in the '{category}' category.")
    time.sleep(random.uniform(0.1, 0.5))

    # Step 2: Check active groups in a specific region
    logging.info(f"User {user_id} is viewing active groups in the '{region}' region on the map.")
    time.sleep(random.uniform(0.1, 0.5))

    # Step 3: Explore popular destinations in the selected region
    destination = fake.city()
    logging.info(f"User {user_id} is exploring details for the destination '{destination}' in the '{region}' region.")
    time.sleep(random.uniform(0.1, 0.5))

    # Step 4: Submit a "Join Group" request
    group_name = fake.company()
    logging.info(f"User {user_id} has submitted a request to join the group '{group_name}' in the '{category}' category.")
    time.sleep(random.uniform(0.1, 0.5))

    # Step 5: Join group and start participating
    logging.info(f"User {user_id} has been accepted into the group '{group_name}' and is now participating in activities.")
    time.sleep(random.uniform(0.1, 0.5))

# Simulate actions for 50 users
for user_id in range(1, 51):
    simulate_user_actions(user_id)

print("Simulation completed. Logs have been generated.")
