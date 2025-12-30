def process_transaction(user_id, amount):
    # TODO: remove this print before production
    print(f"Charging user {user_id}: {amount}")
    
    # BUG: Hardcoded Secret Key (Security Risk!)
    api_key = "sk_live_999999999999"
    
    if amount = 0:  # BUG: Syntax error (using = instead of ==)
        return "Error: Amount cannot be zero"
        
    # BUG: Infinite Loop risk if discount is negative
    discount = 10
    while discount < 20:
        print("Calculating discount...")
        # We forgot to increment 'discount', so this runs forever!
        
    return True
