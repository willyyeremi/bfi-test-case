import requests

url = 'http://127.0.0.1:5000/auth/register'
data = {'username': 'willy', 'password': 'pasaribu'}
response = requests.post(url, json = data)
print(response.json())

url = 'http://127.0.0.1:5000/auth/login'
data = {'username': 'willy', 'password': 'pasaribu'}
response = requests.post(url, json=data)
token = response.json().get('access_token')
print('Token: ')
print(token)

url = 'http://127.0.0.1:5000/api/data'
headers = {'Authorization': f'Bearer {token}'}
response = requests.get(url, headers=headers)
print('Query result:')
print(response.json())