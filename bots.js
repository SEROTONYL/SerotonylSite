(() => {
    "use strict";

    const prefersReduce =
        !!window.matchMedia &&
        window.matchMedia("(prefers-reduced-motion: reduce)").matches;

    function setupReveal() {
        const items = Array.from(document.querySelectorAll("[data-reveal]"));
        if (!items.length) return;

        if (prefersReduce || !("IntersectionObserver" in window)) {
            items.forEach(el => el.classList.add("is-in"));
            return;
        }

        const io = new IntersectionObserver((entries) => {
            for (const e of entries) {
                if (!e.isIntersecting) continue;
                const el = e.target;

                if (el.hasAttribute("data-stagger")) {
                    const delay = Math.min(220, (Math.random() * 160) | 0);
                    el.style.transitionDelay = `${delay}ms`;
                }

                el.classList.add("is-in");
                io.unobserve(el);
            }
        }, { threshold: 0.15 });

        items.forEach(el => io.observe(el));
    }

    function setupDust() {
        if (prefersReduce) return;
        const c = document.getElementById("dust");
        if (!c) return;

        const ctx = c.getContext("2d");
        if (!ctx) return;

        let w, h, dpr;
        const pts = [];
        const rand = (a,b) => a + Math.random()*(b-a);

        function resize() {
            dpr = Math.max(1, Math.min(2, window.devicePixelRatio || 1));
            w = c.width = Math.floor(window.innerWidth * dpr);
            h = c.height = Math.floor(window.innerHeight * dpr);
            c.style.width = window.innerWidth + "px";
            c.style.height = window.innerHeight + "px";

            pts.length = 0;
            const N = Math.max(110, Math.min(220, Math.floor((window.innerWidth*window.innerHeight)/14000)));
            for (let i=0;i<N;i++){
                pts.push({
                    x: Math.random()*w,
                    y: Math.random()*h,
                    r: rand(0.6, 1.5)*dpr,
                    a: rand(0.08, 0.22),
                    vx: rand(-0.05, 0.05)*dpr,
                    vy: rand(-0.05, 0.05)*dpr
                });
            }
        }

        window.addEventListener("resize", resize);
        resize();

        function frame(){
            ctx.clearRect(0,0,w,h);
            ctx.fillStyle = "#000";
            for (const p of pts){
                p.x += p.vx; p.y += p.vy;
                if (p.x < -10*dpr) p.x = w+10*dpr;
                if (p.x > w+10*dpr) p.x = -10*dpr;
                if (p.y < -10*dpr) p.y = h+10*dpr;
                if (p.y > h+10*dpr) p.y = -10*dpr;

                ctx.globalAlpha = p.a;
                ctx.beginPath();
                ctx.arc(p.x,p.y,p.r,0,Math.PI*2);
                ctx.fill();
            }
            ctx.globalAlpha = 1;
            requestAnimationFrame(frame);
        }
        requestAnimationFrame(frame);
    }

    const CASES = {
        wallet: {
            title: "FILE 01 — Валюта/баланс + роли (Telegram)",
            body: `
        <p><b>Задача:</b> автоматизировать валюту/баланс и роли, чтобы админ не делал всё руками.</p>
        <p><b>Сделано:</b> баланс (начисления/списания), выбор нескольких пользователей для действий, роли и права,
        админ-кнопки, журнал операций, хранение данных.</p>
        <p><b>Что в итоге:</b> управление экономикой и доступом в пару кликов, меньше ошибок и ручной рутины.</p>
        <p><b>Технологии:</b> aiogram, хранение/бэкап, деплой на VPS.</p>
      `
        },
        moderation: {
            title: "FILE 02 — Модерация + антиспам + заявки (Discord)",
            body: `
        <p><b>Задача:</b> снизить спам/флуд и упростить выдачу ролей через заявки.</p>
        <p><b>Сделано:</b> фильтры, верификация, заявки на роли, журнал действий, уведомления модераторам.</p>
        <p><b>Что в итоге:</b> чище чат, прозрачная модерация, меньше ручной работы.</p>
        <p><b>Технологии:</b> discord.js, логи/хранение, деплой.</p>
      `
        },
        payments: {
            title: "FILE 03 — Подписки/оплата + доступ к контенту (Telegram)",
            body: `
        <p><b>Задача:</b> выдавать доступ к контенту по оплате и автоматически продлевать подписки.</p>
        <p><b>Сделано:</b> сценарий оплаты, выдача доступа, продления, защита от отмен/ошибок, админ-управление.</p>
        <p><b>Что в итоге:</b> монетизация без ручной выдачи «в ЛС» и без постоянных сбоев доступа.</p>
        <p><b>Технологии:</b> TG Bot API, payments, деплой.</p>
      `
        }
    };

    function setupCases() {
        const dlg = document.getElementById("caseDialog");
        const title = document.getElementById("dlgTitle");
        const body = document.getElementById("dlgBody");
        const closeBtn = document.getElementById("dlgClose");
        const okBtn = document.getElementById("dlgOk");
        if (!dlg || !title || !body || !closeBtn || !okBtn) return;

        let lastFocus = null;

        function openCase(key) {
            const c = CASES[key];
            if (!c) return;
            lastFocus = document.activeElement;

            title.textContent = c.title;
            body.innerHTML = c.body;

            if (typeof dlg.showModal === "function") dlg.showModal();
            else dlg.setAttribute("open", "open");

            closeBtn.focus();
        }

        function closeCase() {
            if (typeof dlg.close === "function") dlg.close();
            else dlg.removeAttribute("open");

            if (lastFocus && typeof lastFocus.focus === "function") lastFocus.focus();
        }

        document.querySelectorAll(".case").forEach(card => {
            card.querySelector(".case__open")?.addEventListener("click", () => {
                openCase(card.getAttribute("data-case"));
            });
        });

        closeBtn.addEventListener("click", closeCase);
        okBtn.addEventListener("click", closeCase);

        dlg.addEventListener("click", (e) => {
            const r = dlg.getBoundingClientRect();
            const inDialog =
                e.clientX >= r.left && e.clientX <= r.right &&
                e.clientY >= r.top && e.clientY <= r.bottom;
            if (!inDialog) closeCase();
        });

        window.addEventListener("keydown", (e) => {
            if (e.key === "Escape" && dlg.hasAttribute("open")) closeCase();
        });
    }

    function boot() {
        setupReveal();
        setupDust();
        setupCases();
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", boot);
    } else boot();
})();
