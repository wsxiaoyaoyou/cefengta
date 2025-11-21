import pytest
import requests
import json
import uuid
from datetime import datetime, timedelta

# 配置测试环境
BASE_URL = "http://127.0.0.1:5000"  # 替换为实际服务地址
TEST_USER = {
	"username": "test_user",
	"password": "test_password"  # 替换为实际测试账号
}

# 全局变量存储认证信息
auth_token = None


@pytest.fixture(scope="session", autouse=True)

def test_hello接口():
	"""测试根路径接口"""
	url = f"{BASE_URL}/"
	response = requests.get(url)
	assert response.status_code == 200
	result = response.json()
	assert result["code"] == 200
	assert result["data"] == "OK"


def test_mastDataShow接口():
	"""测试数据展示接口"""
	url = f"{BASE_URL}/tower_server/atm/mast/mastDataShow"
	params = {
		"pageIndex": 1,
		"pageSize": 10,
	}
	response = requests.post(
		url,
		json=params
	)
	assert response.status_code == 200
	result = response.json()
	assert result["code"] == 200
	assert "mastItems" in result["data"]
	assert "total" in result["data"]


def test_unauthorized访问():
	"""测试未授权访问受保护接口"""
	url = f"{BASE_URL}/tower_server/atm/user/info"
	# 不带token请求
	response = requests.get(url)
	assert response.status_code == 200
	result = response.json()
	assert result["code"] == 401, "未授权访问未被正确拦截"




if __name__ == "__main__":
	pytest.main(["-v", __file__])