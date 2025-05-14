import logging

# Thiết lập logging
logger = logging.getLogger(__name__)

def get_portfolio_advice(portfolio_data):
    """
    Hàm giả lập AI tư vấn danh mục đầu tư
    """
    logger.info("Đang tạo lời khuyên đầu tư giả lập")
    return {
        "advice": "Đây là lời khuyên từ AI: Duy trì danh mục đầu tư cân bằng.",
        "recommended_actions": [
            "Giữ vị thế hiện tại",
            "Theo dõi biến động thị trường"
        ]
    } 