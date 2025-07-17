class Thumbnail:
    BASE_URL = "https://img.youtube.com/vi/"
    QUALITIES = {
        "default": "default.jpg",
        "medium": "mqdefault.jpg",
        "high": "hqdefault.jpg",
        "standard": "sddefault.jpg",
        "maximum": "maxresdefault.jpg",
    }

    def __init__(self, video_id: str):
        self.video_id = video_id

    def get_url(self, quality: str = "default") -> str:
        filename = self.QUALITIES.get(quality)
        if not filename:
            raise ValueError(f"Unknown quality: {quality}")
        return f"{self.BASE_URL}{self.video_id}/{filename}"

    def as_dict(self) -> dict:
        return {quality: self.get_url(quality) for quality in self.QUALITIES}
