import logging
import random
from datetime import datetime

# 설정: 로그 파일과 로그 포맷
logging.basicConfig(
    filename="user_activity.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

def log_event(action, details):
    """로그 이벤트 기록"""
    logging.info(f"ACTION: {action}, DETAILS: {details}")

# 유저 ID (임의 설정)
user_id = random.randint(1000, 9999)

# 1. 메인 화면에서 인기 여행지 섹션 확인
def view_popular_destinations():
    destinations = ["Seoul", "Busan", "Jeju Island", "Gyeongju", "Incheon"]
    log_event("View Popular Destinations", f"User {user_id} viewed popular destinations: {', '.join(destinations)}")

# 2. 특정 여행지를 클릭해 상세 정보와 추천 모임 확인
def view_destination_details(destination):
    recommended_groups = [f"{destination} Explorers", f"{destination} Foodies"]
    log_event("View Destination Details", f"User {user_id} clicked on destination: {destination}. Recommended groups: {', '.join(recommended_groups)}")

# 3. 여행지를 선택해 새로운 모임 생성
def create_new_group(destination, group_name, category):
    log_event("Create New Group", f"User {user_id} created a group '{group_name}' for destination '{destination}' under category '{category}'")

# 4. 모임 관리 탭에서 모임 세부 사항 수정
def edit_group_details(group_name, new_name, new_category, new_destination):
    log_event("Edit Group Details", f"User {user_id} updated group '{group_name}' to name '{new_name}', category '{new_category}', destination '{new_destination}'")

# 5. 모임에 멤버들 모임 신청
def send_join_requests(group_name, members):
    log_event("Send Join Requests", f"User {user_id} sent join requests to members: {', '.join(members)} for group '{group_name}'")

# 6. 신청한 요청 수락한 후 모임에 참여
def accept_join_requests(group_name, accepted_members):
    log_event("Accept Join Requests", f"User {user_id} accepted join requests for group '{group_name}'. Members: {', '.join(accepted_members)}")

def main():
    # Step 1
    view_popular_destinations()

    # Step 2
    selected_destination = "Jeju Island"
    view_destination_details(selected_destination)

    # Step 3
    new_group_name = "Jeju Adventurers"
    category = "Adventure"
    create_new_group(selected_destination, new_group_name, category)

    # Step 4
    edit_group_details(new_group_name, "Jeju Explorers", "Travel & Culture", "Seoul")

    # Step 5
    members = ["member1@example.com", "member2@example.com"]
    send_join_requests("Jeju Explorers", members)

    # Step 6
    accept_join_requests("Jeju Explorers", members)

if __name__ == "__main__":
    main()
