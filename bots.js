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
        if (window.matchMedia && window.matchMedia("(max-width: 720px)").matches) return;
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
            const N = Math.max(80, Math.min(160, Math.floor((window.innerWidth*window.innerHeight)/16000)));
            for (let i=0;i<N;i++){
                pts.push({
                    x: Math.random()*w,
                    y: Math.random()*h,
                    r: rand(0.6, 1.5)*dpr,
                    a: rand(0.05, 0.16),
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
            title: "ЗАДАЧА 01 — Заявки и распределение клиентов",
            body: `
        <p><b>Задача:</b> принимать обращения и сразу направлять их нужному человеку.</p>
        <p><b>Что сделали:</b> короткий сценарий вопросов, сбор контактов, отправка заявки ответственному сотруднику.</p>
        <p><b>Что в итоге:</b> ни одна заявка не теряется, владелец видит порядок и историю обращений.</p>
        <p><b>Дополнительно:</b> хранение истории заявок и запуск на стабильной площадке.</p>
      `
        },
        moderation: {
            title: "ЗАДАЧА 02 — Ответы клиентам и поддержка",
            body: `
        <p><b>Задача:</b> чтобы бот отвечал на частые вопросы и собирал заявки на обратный звонок.</p>
        <p><b>Что сделали:</b> готовые сценарии, кнопки «цены», «услуги», «связаться с человеком».</p>
        <p><b>Что в итоге:</b> клиенты получают ответы сразу, а сотрудники подключаются по нужде.</p>
        <p><b>Дополнительно:</b> журнал обращений и спокойный контроль диалогов.</p>
      `
        },
        payments: {
            title: "ЗАДАЧА 03 — Оплата и выдача доступа",
            body: `
        <p><b>Задача:</b> принимать оплату и давать доступ без ручной проверки.</p>
        <p><b>Что сделали:</b> сценарий оплаты, автоматическая выдача доступа, напоминания о продлении.</p>
        <p><b>Что в итоге:</b> доступ не «ломается», а владелец экономит время.</p>
        <p><b>Дополнительно:</b> отчёт по оплатам и защита от ошибок.</p>
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
        let isClosing = false;

        function openCase(key) {
            const c = CASES[key];
            if (!c) return;
            lastFocus = document.activeElement;
            isClosing = false;
            dlg.classList.remove("closing");

            title.textContent = c.title;
            body.innerHTML = c.body;

            if (typeof dlg.showModal === "function") dlg.showModal();
            else dlg.setAttribute("open", "open");

            closeBtn.focus();
        }

        function closeCase() {
            if (!dlg.hasAttribute("open") || isClosing) return;
            if (prefersReduce) {
                if (typeof dlg.close === "function") dlg.close();
                else dlg.removeAttribute("open");
                if (lastFocus && typeof lastFocus.focus === "function") lastFocus.focus();
                return;
            }
            isClosing = true;
            dlg.classList.add("closing");

            const finishClose = () => {
                dlg.classList.remove("closing");
                if (typeof dlg.close === "function") dlg.close();
                else dlg.removeAttribute("open");
                isClosing = false;
                if (lastFocus && typeof lastFocus.focus === "function") lastFocus.focus();
            };

            const onEnd = (event) => {
                if (event.target !== dlg) return;
                finishClose();
            };
            dlg.addEventListener("animationend", onEnd, { once: true });
            dlg.addEventListener("animationcancel", onEnd, { once: true });
            setTimeout(() => {
                if (isClosing) finishClose();
            }, 240);

        }

        document.querySelectorAll(".case").forEach(card => {
            card.querySelector(".case__open")?.addEventListener("click", () => {
                openCase(card.getAttribute("data-case"));
            });
        });

        closeBtn.addEventListener("click", closeCase);
        okBtn.addEventListener("click", closeCase);
        dlg.addEventListener("cancel", (event) => {
            event.preventDefault();
            closeCase();
        });

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
