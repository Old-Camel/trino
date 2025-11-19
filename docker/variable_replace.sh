#!/bin/bash

# 定义文件夹路径
VAR_DIR=$1
TEMPLATE_DIR=$2

# 对接UNC安全合规，一期的实现方式调用解密服务，解密value,二期时废除此方法
decrypt() {
    # 获取传入的 data 字符串
    key="$1"
    # 获取传入的 data 字符串
    data="$2"

    # 平台的标识 PLATFORM 是环境变量
    # 检查 PLATFORM 是否不为空且为 "unc"，则调用其接口，对加密的配置进行解密
    if [[ -n "$PLATFORM" ]]; then

        # 检查 Key 是否以 "Password" 结尾，或者 value 是否以 "ENC(" 开头且以 ")" 结尾
        # 则调用 decrypt 方法对value进行解密后，重新赋值给value。
        if [[ "$value" =~ ^IDE\(.+\)$ ]] || [[ "$value" =~ ^ENC\(.+\)$ ]]; then
               # 将 data 字符串包装成 JSON 格式
            json_data=$(jq -n --arg data "$data" '{data: $data}')

            # 解密服务地址 ENCRYPTOR_SERVER 是环境变量
            # 发送 POST 请求并获取响应
            response=$(curl -s -w "%{http_code}" -X POST -H "Content-Type: application/json" -d "$json_data" "$ENCRYPTOR_SERVER")

            # 解析响应内容
            http_code="${response: -3}"  # 获取最后三位作为 HTTP 状态码
            response_body="${response:0:${#response}-3}"  # 获取响应体

            # 处理正常返回结果
            if [ "$http_code" -eq 200 ]; then
                result=$(echo "$response_body" | jq -r '.result')
                echo "$result"
            # 处理异常返回结果
            elif [ "$http_code" -eq 500 ]; then
                msg=$(echo "$response_body" | jq -r '.msg')
                echo "【decrypt】=> service ${ENCRYPTOR_SERVER} decrypt data:${data} failed,msg : $msg ..." >&2  # 写入日志
                exit 1
            else
                echo "【decrypt】=> service ${ENCRYPTOR_SERVER} decrypt data:${data} failed,Unexpected response code: $http_code..." >&2  # 写入日志
                exit 1
            fi
        else
            echo "$data"
        fi
    else
        echo "$data"
    fi
}

# 读取 vars 文件夹中的所有 properties 文件
for var_file in "$VAR_DIR"/*.properties; do
    # 读取每个文件中的变量
    while IFS='=' read -r key value; do
        # 清理 key 和 value 的空格
        key=$(echo "$key" | xargs)
        value=$(echo "$value" | xargs)
        #对接UNC安全合规，一期的实现方式调用解密服务，解密value,二期时废除此方法
        value=$(decrypt $key "$value")
        # 使用 sed 进行替换，处理多种占位符格式
        for template_file in "$TEMPLATE_DIR"/*; do
            if [ -d "$template_file" ]; then
              continue
            fi
            sed -i "s|{{ $key }}|$value|g" "$template_file"
        done
    done < "$var_file"
done

echo "配置文件变量替换完成！"
