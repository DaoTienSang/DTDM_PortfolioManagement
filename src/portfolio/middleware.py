from django.conf import settings
from django.contrib.auth import logout

class AdminSessionMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __process_request(self, request):
        if request.user.is_authenticated:
            # Lưu trạng thái admin trong session
            if not request.session.get('is_admin_session') and 'admin' in request.path:
                request.session['is_admin_session'] = True
            
            # Lưu trạng thái người dùng thông thường trong session
            if not request.session.get('is_user_session') and 'admin' not in request.path:
                request.session['is_user_session'] = True
            
            # Nếu người dùng đang trong phiên admin và truy cập trang người dùng
            if request.session.get('is_admin_session') and 'admin' not in request.path:
                logout(request)
                request.session.flush()
                return None
            
            # Nếu người dùng đang trong phiên thông thường và truy cập trang admin
            if request.session.get('is_user_session') and 'admin' in request.path:
                logout(request)
                request.session.flush()
                return None
        
        return None

    def __call__(self, request):
        response = self.__process_request(request)
        if response:
            return response
        
        return self.get_response(request) 