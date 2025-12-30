import os

def process_payment(user_id, amount):
    # BUG 1: Logic Error - This prints the wrong thing and doesn't return anything
    print(f"Processing payment of {amount} for user {user_id}")
    
    # BUG 2: Security Risk - Hardcoded API Key (The AI should SCREAM about this)
    stripe_api_key = "sk_live_1234567890abcdefghijklmnop" 
    
    return True

def calculate_discount(price, discount):
    # BUG 3: Potential Crash - Division by zero if discount is 0
    final_price = price / discount
    return final_price

# BUG 4: Running code at top level without if __name__ == "__main__":
print(process_payment("user_1", 100))
print(calculate_discount(100, 0))
