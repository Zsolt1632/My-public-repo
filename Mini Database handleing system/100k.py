import random
import string
import json

# Function to generate a random string of given length
def random_string(length):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for _ in range(length))

# Function to generate a random email
def random_email():
    domains = ["example.com", "test.com", "dummy.com"]
    return random_string(7) + "@" + random.choice(domains)

# Function to generate JSON documents for students
def generate_student_docs(num_docs):
    docs = []
    for i in range(1, num_docs + 1):
        studid = i
        groupid = random.randint(1, 10)  # Assuming there are 10 groups
        studname = random_string(10)
        email = random_email()
        doc = {
            "_id": f"{studid}",
            "Value": f"{groupid}#{studname}#{email}"
        }
        docs.append(doc)
    return docs

# Generate 100k student documents
student_docs = generate_student_docs(100000)

# Write the documents to a JSON file
with open('student_documents.json', 'w') as file:
    json.dump(student_docs, file)

print("Done generating student documents.")
