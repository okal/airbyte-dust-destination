#!/usr/bin/env python3
"""
Generate a test CSV file with personal information for 50 people.
"""

import csv
import random
from datetime import datetime, timedelta

# Sample data pools
first_names = [
    "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
    "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Charles", "Karen", "Christopher", "Nancy", "Daniel", "Lisa",
    "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra", "Donald", "Ashley",
    "Steven", "Kimberly", "Paul", "Emily", "Andrew", "Donna", "Joshua", "Michelle",
    "Kenneth", "Carol", "Kevin", "Amanda", "Brian", "Dorothy", "George", "Melissa",
    "Timothy", "Deborah", "Ronald", "Stephanie", "Jason", "Rebecca", "Edward", "Sharon"
]

last_names = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Wilson", "Anderson", "Thomas", "Taylor",
    "Moore", "Jackson", "Martin", "Lee", "Thompson", "White", "Harris", "Sanchez",
    "Clark", "Ramirez", "Lewis", "Robinson", "Walker", "Young", "Allen", "King",
    "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores", "Green", "Adams",
    "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell", "Carter", "Roberts"
]

cities = [
    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio",
    "San Diego", "Dallas", "San Jose", "Austin", "Jacksonville", "Fort Worth", "Columbus",
    "Charlotte", "San Francisco", "Indianapolis", "Seattle", "Denver", "Washington",
    "Boston", "El Paso", "Nashville", "Detroit", "Oklahoma City", "Portland", "Las Vegas",
    "Memphis", "Louisville", "Baltimore", "Milwaukee", "Albuquerque", "Tucson", "Fresno",
    "Sacramento", "Kansas City", "Mesa", "Atlanta", "Omaha", "Colorado Springs"
]

states = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN",
    "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV",
    "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN",
    "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
]

occupations = [
    "Software Engineer", "Teacher", "Nurse", "Accountant", "Marketing Manager",
    "Sales Representative", "Project Manager", "Designer", "Lawyer", "Doctor",
    "Engineer", "Consultant", "Analyst", "Manager", "Director", "Administrator",
    "Coordinator", "Specialist", "Assistant", "Executive", "Developer", "Architect"
]

def generate_phone():
    """Generate a random phone number."""
    return f"{random.randint(200, 999)}-{random.randint(200, 999)}-{random.randint(1000, 9999)}"

def generate_email(first_name, last_name):
    """Generate an email address."""
    domains = ["gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "company.com"]
    return f"{first_name.lower()}.{last_name.lower()}@{random.choice(domains)}"

def generate_date_of_birth():
    """Generate a random date of birth (age between 18 and 80)."""
    age = random.randint(18, 80)
    birth_date = datetime.now() - timedelta(days=age * 365 + random.randint(0, 365))
    return birth_date.strftime("%Y-%m-%d")

def generate_zip_code():
    """Generate a random ZIP code."""
    return f"{random.randint(10000, 99999)}"

def generate_street_address():
    """Generate a random street address."""
    street_numbers = [str(random.randint(100, 9999))]
    street_names = [
        "Main St", "Oak Ave", "Park Blvd", "Maple Dr", "Elm St", "Cedar Ln",
        "Pine Rd", "First St", "Second Ave", "Washington St", "Lincoln Ave",
        "Jefferson Dr", "Madison Blvd", "Monroe St", "Adams Ave"
    ]
    return f"{random.choice(street_numbers)} {random.choice(street_names)}"

def main():
    """Generate the CSV file."""
    output_file = "test_people.csv"
    
    with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
        fieldnames = [
            "id", "first_name", "last_name", "email", "phone", "street_address",
            "city", "state", "zip_code", "date_of_birth", "age", "occupation"
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for i in range(1, 51):
            first_name = random.choice(first_names)
            last_name = random.choice(last_names)
            birth_date = generate_date_of_birth()
            age = (datetime.now() - datetime.strptime(birth_date, "%Y-%m-%d")).days // 365
            
            writer.writerow({
                "id": i,
                "first_name": first_name,
                "last_name": last_name,
                "email": generate_email(first_name, last_name),
                "phone": generate_phone(),
                "street_address": generate_street_address(),
                "city": random.choice(cities),
                "state": random.choice(states),
                "zip_code": generate_zip_code(),
                "date_of_birth": birth_date,
                "age": age,
                "occupation": random.choice(occupations)
            })
    
    print(f"Generated {output_file} with 50 records")

if __name__ == "__main__":
    main()
