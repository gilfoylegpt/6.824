#!/bin/bash

# 遍历当前目录下的 test-*.log 文件
for file in test-*.log; do
    [ -e "$file" ] || continue  # 防止没有匹配项时报错

    # 提取编号，例如 test-12.log -> 12
    num=${file#test-}
    num=${num%.log}

    # 读取最后一行
    last_line=$(tail -n 1 "$file")

    # 判断最后一行是否以 PASS 结尾
    if [[ ! "$last_line" =~ PASS$ ]]; then
        echo "❌ $file 不以 PASS 结尾，复制相关文件..."

        # 复制 log
        cp "$file" "${num}.log"

        # 若存在对应的 err 文件则复制
        if [[ -f "test-${num}.err" ]]; then
            cp "test-${num}.err" "${num}.err"
        else
            echo "⚠️  test-${num}.err 不存在，跳过"
        fi
    fi
done

echo "✅ 处理完成。"
