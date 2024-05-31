#!/usr/bin/env python3
"""

author: Enoc Martínez
institution: Universitat Politècnica de Catalunya (UPC)
email: enoc.martinez@upc.edu
license: MIT
created: 29/5/24
"""

from argparse import ArgumentParser
from PIL import Image, ImageDraw

# Step 1: Create a new blank image
width, height = 500, 5000  # Define the dimensions of the image
image = Image.new('RGB', (width, height), 'white')  # Create a white background image

# Step 2: Initialize the drawing context
draw = ImageDraw.Draw(image)

# Step 3: Define the rectangle coordinates and draw it
top_left = (50, 50)  # Top-left corner of the rectangle
bottom_right = (150, 150)  # Bottom-right corner of the rectangle
rectangle_color = 'blue'  # Color of the rectangle
draw.rectangle([top_left, bottom_right], fill=rectangle_color)

# Step 4: Save or display the image
image.save('rectangle_image.jpg')  # Save the image to a file
