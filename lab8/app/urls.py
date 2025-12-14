from django.urls import path
from app import views

urlpatterns = [
    path('analysis/', views.perform_calculation, name='calc'),
]
