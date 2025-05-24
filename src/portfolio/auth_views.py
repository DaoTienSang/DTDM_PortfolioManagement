from django.conf import settings
from django.urls import reverse
from django.shortcuts import redirect
from django.contrib.auth import login, logout
from authlib.integrations.django_client import OAuth
from urllib.parse import quote_plus, urlencode
import json 
from .models import User, Wallet 
from .utils import get_auth0_user_profile

def login_view(request):
    """Sử dụng Auth0 để xác thực người dùng"""
    # Khởi tạo OAuth client
    oauth = OAuth()
    oauth.register(
        "auth0",
        client_id=settings.AUTH0_CLIENT_ID,
        client_secret=settings.AUTH0_CLIENT_SECRET,
        client_kwargs={
            "scope": "openid profile email",
        },
        server_metadata_url=f"https://{settings.AUTH0_DOMAIN}/.well-known/openid-configuration",
    )
    
    # Chuyển hướng đến trang đăng nhập Auth0
    return oauth.auth0.authorize_redirect(
        request, request.build_absolute_uri(reverse("callback"))
    )

def callback(request):
    """Xử lý callback từ Auth0 và đăng nhập người dùng"""
    # Khởi tạo OAuth client
    oauth = OAuth()
    oauth.register(
        "auth0",
        client_id=settings.AUTH0_CLIENT_ID,
        client_secret=settings.AUTH0_CLIENT_SECRET,
        client_kwargs={
            "scope": "openid profile email",
        },
        server_metadata_url=f"https://{settings.AUTH0_DOMAIN}/.well-known/openid-configuration",
    )
    
    # Lấy token từ Auth0
    token = oauth.auth0.authorize_access_token(request)
    userinfo = token.get('userinfo')
    
    # Lấy thêm thông tin từ API Auth0 nếu được
    access_token = token.get('access_token')
    if access_token:
        additional_info = get_auth0_user_profile(access_token)
        if additional_info:
            # Merge additional info vào userinfo
            userinfo.update(additional_info)
    
    if userinfo:
        # Lưu thông tin người dùng vào session
        request.session['userinfo'] = userinfo
        
        # Kiểm tra xem người dùng đã tồn tại trong database chưa
        auth0_user_id = userinfo.get('sub')
        email = userinfo.get('email')
        
        try:
            # Tìm user bằng auth0_user_id
            user = User.objects.get(auth0_user_id=auth0_user_id)
        except User.DoesNotExist:
            try:
                # Tìm user bằng email nếu không tìm thấy qua auth0_user_id
                user = User.objects.get(email=email)
                user.auth0_user_id = auth0_user_id
                user.save()
            except User.DoesNotExist:
                # Tạo user mới nếu chưa tồn tại
                user = User.objects.create_user(
                    username=email.split('@')[0], # Cần username, lấy tạm phần đầu email
                    email=email,
                    auth0_user_id=auth0_user_id,
                    first_name=userinfo.get('given_name', ''),
                    last_name=userinfo.get('family_name', ''),
                    profile_picture_url=userinfo.get('picture', '')
                )
                # Tự động tạo ví điện tử cho người dùng mới
                Wallet.objects.create(user=user)
        
        # Đăng nhập người dùng
        login(request, user)
        
        # Cập nhật thông tin người dùng từ Auth0 profile
        if 'picture' in userinfo and userinfo['picture']:
            user.profile_picture_url = userinfo['picture']
        if 'given_name' in userinfo and userinfo['given_name']:
            user.first_name = userinfo['given_name']
        if 'family_name' in userinfo and userinfo['family_name']:
            user.last_name = userinfo['family_name']
        
        for field in user._meta.fields:
            value = getattr(user, field.name)
            if isinstance(value, str) and len(value) > 200: 
                print(f"Field {field.name} quá dài: {len(value)} ký tự, sẽ được cắt bớt hoặc xử lý.")

        user.save()
        
        return redirect('dashboard')
    
    return redirect('home')

def logout_view(request):
    """Đăng xuất khỏi Django và Auth0"""
    # Đăng xuất khỏi Django
    logout(request)
    
    # Xóa session
    request.session.clear()
    
    # Chuyển hướng đến trang đăng xuất của Auth0
    return redirect(
        f"https://{settings.AUTH0_DOMAIN}/v2/logout?"
        + urlencode(
            {
                "returnTo": request.build_absolute_uri(reverse("home")),
                "client_id": settings.AUTH0_CLIENT_ID,
            },
            quote_via=quote_plus,
        )
    )

def register(request):
    """Chuyển hướng đến trang đăng ký của Auth0"""
    # Sử dụng cùng hàm login_view để chuyển hướng đến Auth0
    return login_view(request) 