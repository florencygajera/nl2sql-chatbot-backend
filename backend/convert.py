with open("server_logs.txt", "r", encoding="utf-16le") as f:
    text = f.read()
with open("trace.txt", "w", encoding="utf-8") as f:
    f.write(text)
