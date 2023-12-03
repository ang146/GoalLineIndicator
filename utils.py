def trim_string(to_trim :str) -> str:
    trimmed = to_trim.replace("1", "").replace("2", "").replace("3", "").replace("4", "").replace("5", "").replace("6", "").replace("7", "").strip()
    return trimmed

def get_goals(to_trim :str) -> int:
    trimmeds = to_trim.strip().replace('(', '').replace(')', '').split('-')
    goals = 0
    for trimmed in trimmeds:
        goals += int(trimmed.strip())
    
    return goals

def half_to_full_width(input_string :str):
    half_width_chars = ''.join(chr(i) for i in range(0x0021, 0x007F))  # Half-width ASCII characters
    full_width_chars = ''.join(chr(i) for i in range(0xFF01, 0xFF5F))  # Full-width ASCII characters
    translation_table = str.maketrans(half_width_chars, full_width_chars)
    return input_string.translate(translation_table)