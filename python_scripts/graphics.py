import io
import matplotlib.pyplot as plt
import matplotlib.image as mpimg


def showImg(imgbytes: bytes, format: str = "png"):
    fp = io.BytesIO(imgbytes)
    with fp:
        img = mpimg.imread(fp, format=format)
    plt.imshow(img)
    plt.grid(False)
    plt.axis("off")
    plt.show()
