import csv

# 输入和输出文件的路径
input_file_path = r"0.txt"
output_file_path = r'1.csv'

# 使用制表符作为分隔符
delimiter = '\t'

# 打开输入文件和输出文件
with open(input_file_path, 'r', encoding='utf-8') as infile, \
        open(output_file_path, 'w', newline='', encoding='utf-8') as outfile:
    reader = csv.reader(infile, delimiter=delimiter)
    writer = csv.writer(outfile)

    # 逐行读取并写入
    for row in reader:
        writer.writerow(row)

print("转换完成，输出文件位于：" + output_file_path)
