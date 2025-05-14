#!/usr/bin/env python3

with open('views.py', 'r') as f:
    lines = f.readlines()

# Sửa lỗi thụt đầu dòng
for i in range(len(lines)):
    if "except Exception as e:" in lines[i] and lines[i].startswith("    except"):
        lines[i] = lines[i].replace("    except", "        except")
    
    # Sửa các lỗi thụt đầu dòng khác
    if "fallback_data = [" in lines[i] and not lines[i].startswith("            "):
        lines[i] = lines[i].replace("fallback_data = [", "            fallback_data = [")
        
    if "price_board_json = json.dumps(fallback_data)" in lines[i] and not lines[i].startswith("            "):
        lines[i] = lines[i].replace("price_board_json = json.dumps(fallback_data)", "            price_board_json = json.dumps(fallback_data)")
        
    if "context = {" in lines[i] and not lines[i].startswith("            "):
        lines[i] = lines[i].replace("context = {", "            context = {")

# Lưu file đã sửa
with open('views_fixed.py', 'w') as f:
    f.writelines(lines)

print("Đã sửa xong và lưu vào views_fixed.py") 