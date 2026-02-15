import random
def omikuji():
    kuji=["大吉", "中吉", "小吉", "吉", "末吉", "凶"]
    return random.choice(kuji)
    kekka=omikuji
print("結果は"+omikuji+"です。")