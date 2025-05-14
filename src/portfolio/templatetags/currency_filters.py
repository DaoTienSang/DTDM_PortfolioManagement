from django import template

register = template.Library()

@register.filter(name='dinh_dang_tien')
def dinh_dang_tien(so):
    """
    Format a number as currency with dot as thousand separator and comma as decimal separator.
    Example: 1234567.89 -> 1.234.567,89
    """
    if so is None:
        so = 0
    
    # Convert to float to ensure consistent handling
    try:
        so = float(so)
    except (ValueError, TypeError):
        so = 0
        
    # Format with thousand separators and 2 decimal places
    formatted = "{:,.0f}".format(so)  # No decimal places for currency
    
    # Replace commas with dots for thousand separators
    return formatted.replace(",", ".")

@register.filter(name='maximum')
def maximum(value, arg):
    """
    Returns the maximum value between the value and the argument.
    This is useful to ensure a value is not less than a minimum value.
    
    Example: {{ value|maximum:0 }} will return 0 if value is negative
    """
    try:
        value = float(value)
        arg = float(arg)
        return max(value, arg)
    except (ValueError, TypeError):
        return arg

@register.filter(name='percentage')
def percentage(value, total):
    """
    Calculates percentage of value relative to total.
    Returns 0 if total is 0 or if there's an error.
    
    Example: {{ 25|percentage:100 }} will return 25.0
    """
    try:
        value = float(value)
        total = float(total)
        if total > 0:
            return (value / total) * 100
        return 0
    except (ValueError, TypeError, ZeroDivisionError):
        return 0

@register.filter(name='profit_percentage')
def profit_percentage(profit, cost):
    """
    Calculates profit percentage relative to cost.
    Returns 0 if cost is 0 or if there's an error.
    
    Example: {{ 250|profit_percentage:1000 }} will return 25.0
    """
    try:
        profit = float(profit)
        cost = float(cost)
        if cost > 0:
            return (profit / cost) * 100
        return 0
    except (ValueError, TypeError, ZeroDivisionError):
        return 0

@register.filter(name='progress')
def progress(current, target):
    """
    Calculates progress percentage towards a target.
    Ensures the progress doesn't exceed 100%.
    Returns 0 if target is 0 or if there's an error.
    
    Example: {{ 75|progress:100 }} will return 75.0
    """
    try:
        current = float(current)
        target = float(target)
        if target > 0:
            progress = (current / target) * 100
            return min(progress, 100)  # Cap at 100%
        return 0
    except (ValueError, TypeError, ZeroDivisionError):
        return 0
        
@register.filter(name='format_percentage')
def format_percentage(value, decimal_places=2):
    """
    Formats a number as a percentage with the specified decimal places.
    
    Example: {{ 25.123|format_percentage:1 }} will return "25.1%"
    """
    try:
        value = float(value)
        format_str = "{:." + str(decimal_places) + "f}"
        return format_str.format(value) + "%"
    except (ValueError, TypeError):
        return "0" + "%"

@register.filter(name='add_sign')
def add_sign(value):
    """
    Adds '+' sign to positive numbers, keeps '-' sign for negative numbers.
    
    Example: {{ 25|add_sign }} will return "+25"
             {{ -25|add_sign }} will return "-25"
    """
    try:
        value = float(value)
        if value > 0:
            return "+" + str(value)
        return str(value)
    except (ValueError, TypeError):
        return "0"

@register.filter(name='add_sign_to_formatted')
def add_sign_to_formatted(formatted_value):
    """
    Adds '+' sign to positive formatted numbers if not already present.
    Useful for already formatted values like {{ value|floatformat:2 }}
    
    Example: {{ "25.50"|add_sign_to_formatted }} will return "+25.50"
             {{ "-25.50"|add_sign_to_formatted }} will return "-25.50"
    """
    if not formatted_value:
        return "0"
    
    formatted_value = str(formatted_value)
    if formatted_value.startswith('-'):
        return formatted_value
    elif formatted_value.startswith('+'):
        return formatted_value
    else:
        return "+" + formatted_value 