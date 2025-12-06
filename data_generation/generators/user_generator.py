"""
User Profile Generator for GlobalMart
Generates realistic user profiles with demographics and preferences
"""
import random
import json
from datetime import datetime, timedelta
from faker import Faker
from typing import List, Dict
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.settings import settings
from config.constants import COUNTRIES, USER_PREFERENCES, AGE_RANGES


class UserGenerator:
    """Generates realistic user profiles"""

    def __init__(self, num_users: int = None):
        self.faker = Faker()
        self.num_users = num_users or settings.data_generation.num_users
        self.generated_users = []

    def generate_age(self) -> int:
        """Generate age based on demographic distribution"""
        age_range = random.choices(
            list(AGE_RANGES.keys()),
            weights=list(AGE_RANGES.values()),
            k=1
        )[0]

        # Map age range to actual age
        range_map = {
            "18-24": (18, 24),
            "25-34": (25, 34),
            "35-44": (35, 44),
            "45-54": (45, 54),
            "55+": (55, 75)
        }

        min_age, max_age = range_map[age_range]
        return random.randint(min_age, max_age)

    def generate_preferences(self) -> List[str]:
        """Generate user shopping preferences"""
        # Users typically have 1-3 preference categories
        num_preferences = random.randint(1, 3)
        return random.sample(USER_PREFERENCES, num_preferences)

    def generate_registration_date(self) -> str:
        """Generate registration date (within last 5 years)"""
        days_ago = random.randint(0, 5 * 365)
        reg_date = datetime.utcnow() - timedelta(days=days_ago)
        return reg_date.isoformat()

    def generate_user(self, user_id: int) -> Dict:
        """Generate a single user profile"""
        user = {
            "user_id": f"USER_{user_id:08d}",
            "email": self.faker.email(),
            "age": self.generate_age(),
            "country": random.choice(COUNTRIES),
            "registration_date": self.generate_registration_date(),
            "preferences": self.generate_preferences()
        }
        return user

    def generate_batch(self, batch_size: int = 1000) -> List[Dict]:
        """Generate a batch of users"""
        users = []
        for i in range(batch_size):
            users.append(self.generate_user(len(self.generated_users) + i + 1))
        return users

    def generate_all(self) -> List[Dict]:
        """Generate all users"""
        print(f"Generating {self.num_users} user profiles...")

        batch_size = 10000
        all_users = []

        for batch_num in range(0, self.num_users, batch_size):
            current_batch_size = min(batch_size, self.num_users - batch_num)
            batch = self.generate_batch(current_batch_size)
            all_users.extend(batch)

            if (batch_num + current_batch_size) % 10000 == 0:
                print(f"Generated {batch_num + current_batch_size}/{self.num_users} users...")

        self.generated_users = all_users
        print(f"✓ Generated {len(all_users)} user profiles")
        return all_users

    def save_to_file(self, filepath: str = "data/raw/users.json"):
        """Save generated users to JSON file"""
        if not self.generated_users:
            self.generate_all()

        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        with open(filepath, 'w') as f:
            json.dump(self.generated_users, f, indent=2)

        print(f"✓ Saved {len(self.generated_users)} users to {filepath}")

    def load_from_file(self, filepath: str = "data/raw/users.json") -> List[Dict]:
        """Load users from JSON file"""
        if os.path.exists(filepath):
            with open(filepath, 'r') as f:
                self.generated_users = json.load(f)
            print(f"✓ Loaded {len(self.generated_users)} users from {filepath}")
            return self.generated_users
        else:
            print(f"✗ File not found: {filepath}")
            return []

    def get_random_user(self) -> Dict:
        """Get a random user from generated users"""
        if not self.generated_users:
            self.load_from_file()

        if not self.generated_users:
            raise ValueError("No users available. Generate users first.")

        return random.choice(self.generated_users)


if __name__ == "__main__":
    # Test the generator
    generator = UserGenerator(num_users=10)
    users = generator.generate_all()

    print("\nSample users:")
    for user in users[:3]:
        print(json.dumps(user, indent=2))

    generator.save_to_file()
