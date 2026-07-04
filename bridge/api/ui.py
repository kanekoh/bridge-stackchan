"""Web UI endpoints."""
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from bridge.core.db import _get_setting
from bridge.config import SPEAKER_ID_BROWSER_URL

router = APIRouter()
_templates = Jinja2Templates(directory="templates")


def _ui_context(request: Request, **extra) -> dict:
    """全テンプレートに渡す共通コンテキスト。"""
    return {
        "speaker_id_browser_url": _get_setting("speaker_id_browser_url", SPEAKER_ID_BROWSER_URL),
        **extra,
    }


@router.get("/ui", response_class=HTMLResponse)
async def ui_index():
    return RedirectResponse(url="/ui/dashboard")


@router.get("/ui/dashboard", response_class=HTMLResponse)
async def ui_dashboard(request: Request):
    return _templates.TemplateResponse(request=request, name="dashboard.html", context=_ui_context(request))


@router.get("/ui/device", response_class=HTMLResponse)
async def ui_device(request: Request):
    return _templates.TemplateResponse(request=request, name="device.html", context=_ui_context(request))


@router.get("/ui/members", response_class=HTMLResponse)
async def ui_members(request: Request):
    return _templates.TemplateResponse(request=request, name="members.html", context=_ui_context(request))


@router.get("/ui/messages", response_class=HTMLResponse)
async def ui_messages(request: Request):
    return _templates.TemplateResponse(request=request, name="messages.html", context=_ui_context(request))


@router.get("/ui/test", response_class=HTMLResponse)
async def ui_test(request: Request):
    return _templates.TemplateResponse(request=request, name="test.html", context=_ui_context(request))


@router.get("/ui/settings", response_class=HTMLResponse)
async def ui_settings(request: Request):
    return _templates.TemplateResponse(request=request, name="settings.html", context=_ui_context(request))


@router.get("/ui/notifications", response_class=HTMLResponse)
async def ui_notifications(request: Request):
    return _templates.TemplateResponse(request=request, name="notifications.html", context=_ui_context(request))


@router.get("/ui/logs", response_class=HTMLResponse)
async def ui_logs(request: Request):
    return _templates.TemplateResponse(request=request, name="logs.html", context=_ui_context(request))


@router.get("/ui/web-checks", response_class=HTMLResponse)
async def ui_web_checks(request: Request):
    return _templates.TemplateResponse(request=request, name="web_checks.html", context=_ui_context(request))


@router.get("/ui/metrics", response_class=HTMLResponse)
async def ui_metrics(request: Request):
    return _templates.TemplateResponse(request=request, name="metrics.html", context=_ui_context(request))
