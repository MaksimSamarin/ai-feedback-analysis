# Custom logo

Place `logo.png` or `logo.svg` in this directory as a local override. Analyzer uses the PNG first, then the SVG, then its built-in pixel logo.

Compose mounts this directory in the frontend as read-only. Reload the page to see the new logo.

To change the tab icon, replace `frontend/public/review-pixel.png` and rebuild the frontend.

```sh
docker compose up -d --build frontend
```
