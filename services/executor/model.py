def generate_response(review):
    review_lower = review.lower()
    if "не работает" in review_lower:
        return "Приносим свои извинения! Проверьте, правильно ли подключено устройство. Если проблема останется, свяжитесь с поддержкой по email help@3dubka.ru"
    elif "отличный" in review_lower or "спасибо" in review_lower or "быстрая доставка" in review_lower:
        return "Спасибо за высокую оценку! Будем рады видеть вас снова!"
    elif "качество" in review_lower or "не соответствует" in review_lower:
        return "Благодарим за обратную связь! Мы обязательно передадим ваши замечания в отдел контроля качества."
    else:
        return "Спасибо за ваш отзыв! Мы обязательно учтём ваши пожелания и постараемся улучшить сервис."