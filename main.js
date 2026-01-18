(() => {
  "use strict";

  const prefersReduce =
    !!window.matchMedia &&
    window.matchMedia("(prefers-reduced-motion: reduce)").matches;

  const clamp = (v, a, b) => Math.max(a, Math.min(b, v));
  const lerp = (a, b, t) => a + (b - a) * t;
  const smoothstep = (a, b, x) => {
    const t = clamp((x - a) / (b - a), 0, 1);
    return t * t * (3 - 2 * t);
  };

  // shared "mood" state between systems
  const state = {
    anx: 0,
    camx: 0,
    camy: 0,
    blackoutUntil: 0,
    spikeUntil: 0,
    rippleOff: false,
    audioStarted: false,
    audio: null,
  };

  const root = document.documentElement;
  const setRootVar = (k, v) => root.style.setProperty(k, v);

  // ---------------- UI: "убрать рябь" ----------------
  function setupRippleToggle() {
    const cb = document.getElementById("toggleRipple");
    if (!cb) return;

    let off = false;
    try {
      off = localStorage.getItem("serotonyl_ripple_off") === "1";
    } catch {
      off = false;
    }

    cb.checked = off;
    state.rippleOff = off;
    document.body.classList.toggle("ripple-off", off);

    cb.addEventListener("change", () => {
      const v = !!cb.checked;
      state.rippleOff = v;
      document.body.classList.toggle("ripple-off", v);
      try {
        localStorage.setItem("serotonyl_ripple_off", v ? "1" : "0");
      } catch {}
    });
  }

  // ---------------- LOGO TEXT: more OMORI-ish ----------------
  function enhanceSecretLogoText() {
    const logo = document.getElementById("secretLogo");
    if (!logo) return;

    const main = logo.querySelector(".secret-logo__main");
    if (!main) return;

    if (logo.dataset.enhanced === "1") return;
    logo.dataset.enhanced = "1";

    const text = (main.textContent || "").trim();
    if (!text) return;

    main.textContent = "";
    main.classList.add("is-split");

    for (const ch of text) {
      const s = document.createElement("span");
      s.className = "ch";
      s.textContent = ch === " " ? "\u00A0" : ch;
      main.appendChild(s);
    }
  }

  // ---------------- DOORS ----------------
  function setupDoors() {
    const doors = document.querySelectorAll(".door");
    if (!doors.length) return;

    doors.forEach((door) => {
      const rig = door.querySelector(".door__rig");
      if (!rig) return;

      let hover = false;

      const step = () => {
        if (prefersReduce) return;

        // лёгкая дрожь, но теперь зависит от тревожности (state.anx)
        const a = state.anx;
        const k = (hover ? 1.15 : 1.0) * (1 + a * 0.55);

        rig.style.setProperty("--jx", `${(Math.random() * 2 - 1) * k}px`);
        rig.style.setProperty("--jy", `${(Math.random() * 2 - 1) * k}px`);
        rig.style.setProperty("--jr", `${(Math.random() * 0.55 - 0.275) * k}deg`);
        rig.style.setProperty("--js", `${hover ? 1.03 : 1.0}`);

        setTimeout(step, 120 + Math.random() * 160);
      };

      door.addEventListener("pointerenter", () => (hover = true));
      door.addEventListener("pointerleave", () => (hover = false));

      step();

      // мобилки: первый тап открывает, второй переходит
      door.addEventListener(
        "touchstart",
        (e) => {
          if (!door.classList.contains("is-open")) {
            e.preventDefault();
            doors.forEach((d) => d.classList.remove("is-open"));
            door.classList.add("is-open");
          }
        },
        { passive: false }
      );
    });

    document.addEventListener("click", (e) => {
      const isOnDoor = [...doors].some((d) => d.contains(e.target));
      if (!isOnDoor) doors.forEach((d) => d.classList.remove("is-open"));
    });
  }

  // ---------------- DUST + GLITCH (canvas) ----------------
  function setupDustCanvas() {
    if (prefersReduce) return;

    const c = document.getElementById("dust");
    if (!c) return;

    const ctx = c.getContext("2d");
    if (!ctx) return;

    // буфер: сначала пыль туда, потом tearing на основной
    const buf = document.createElement("canvas");
    const bctx = buf.getContext("2d");
    if (!bctx) return;

    let w = 0,
      h = 0,
      dpr = 1;

    // базовые настройки (дальше модулируем от state.anx)
    const GLITCH_RATE_BASE = 0.03;
    const GLITCH_FRAMES_MIN = 3;
    const GLITCH_FRAMES_MAX = 8;

    function resize() {
      dpr = Math.max(1, Math.min(2, window.devicePixelRatio || 1));
      w = c.width = Math.floor(window.innerWidth * dpr);
      h = c.height = Math.floor(window.innerHeight * dpr);
      c.style.width = window.innerWidth + "px";
      c.style.height = window.innerHeight + "px";

      buf.width = w;
      buf.height = h;
    }

    window.addEventListener("resize", resize);
    resize();

    const N = Math.max(
      220,
      Math.min(620, Math.floor((window.innerWidth * window.innerHeight) / 5600))
    );

    const pts = Array.from({ length: N }, () => ({
      x: Math.random() * w,
      y: Math.random() * h,
      r: (Math.random() * 1.2 + 0.6) * dpr,
      vx: (Math.random() * 0.18 - 0.09) * dpr,
      vy: (Math.random() * 0.18 - 0.09) * dpr,
      a: 0.18 + Math.random() * 0.55,
    }));

    let glitchFrames = 0;
    let last = 0;

    function startGlitch() {
      glitchFrames =
        GLITCH_FRAMES_MIN +
        Math.floor(Math.random() * (GLITCH_FRAMES_MAX - GLITCH_FRAMES_MIN + 1));
    }

    function draw(t) {
      requestAnimationFrame(draw);
      if (t - last < 1000 / 60) return;
      last = t;

      // Пользователь может вырубить рябь/сканлайны (для чувствительных глаз).
      // Тогда просто очищаем канвас и не рисуем глитч.
      if (state.rippleOff) {
        ctx.clearRect(0, 0, w, h);
        return;
      }

      const a = state.anx;
      const blackout = t < state.blackoutUntil;

      // чуть сильнее пыль/глитч когда тревожно
      const rate = GLITCH_RATE_BASE + a * 0.11;

      if (glitchFrames === 0 && Math.random() < rate) startGlitch();
      const glitch = glitchFrames > 0;
      if (glitch) glitchFrames--;

      // динамическая прозрачность канваса
      setRootVar("--dustOp", (0.18 + a * 0.34 + (blackout ? 0.08 : 0)).toFixed(3));

      // 1) пыль в буфер
      bctx.clearRect(0, 0, w, h);
      bctx.fillStyle = "#000";

      for (const p of pts) {
        p.x += p.vx;
        p.y += p.vy;

        if ((glitch || blackout) && Math.random() < 0.28 + a * 0.22) {
          p.x += (Math.random() * 36 - 18) * dpr;
          p.y += (Math.random() * 26 - 13) * dpr;
        }

        if (p.x < -20 * dpr) p.x = w + 20 * dpr;
        if (p.x > w + 20 * dpr) p.x = -20 * dpr;
        if (p.y < -20 * dpr) p.y = h + 20 * dpr;
        if (p.y > h + 20 * dpr) p.y = -20 * dpr;

        bctx.globalAlpha = p.a * (glitch ? 1.25 : 1.0) * (blackout ? 1.25 : 1.0);
        bctx.beginPath();
        bctx.arc(p.x, p.y, p.r, 0, Math.PI * 2);
        bctx.fill();
      }
      bctx.globalAlpha = 1;

      // 2) основной
      ctx.clearRect(0, 0, w, h);
      ctx.globalAlpha = 1;
      ctx.globalCompositeOperation = "source-over";
      ctx.drawImage(buf, 0, 0);

      if (glitch || blackout) {
        // tearing полосы
        const bands = 3 + Math.floor(Math.random() * (4 + a * 6));
        for (let i = 0; i < bands; i++) {
          const bandH = (1 + Math.random() * (6 + a * 10)) * dpr;
          const yy = Math.random() * (h - bandH);
          const dx = (Math.random() * 2 - 1) * (70 + a * 120) * dpr;

          ctx.globalAlpha = 0.95;
          ctx.drawImage(buf, 0, yy, w, bandH, dx, yy, w, bandH);

          // тонкая “царапина”
          ctx.globalAlpha = 0.22;
          ctx.fillStyle = "#000";
          ctx.fillRect(0, yy + bandH, w, 1 * dpr);
        }

        // сканлайны
        ctx.globalAlpha = 0.16 + a * 0.14 + (blackout ? 0.10 : 0);
        ctx.fillStyle = "#000";
        const lines = 7 + Math.floor(Math.random() * (7 + a * 12));
        for (let i = 0; i < lines; i++) {
          const y = Math.random() * h;
          const hh = (1 + Math.floor(Math.random() * 3)) * dpr;
          ctx.fillRect(0, y, w, hh);
        }

        // блоки (компрессия/вхс)
        ctx.globalAlpha = 0.10 + a * 0.08;
        const blocks = 4 + Math.floor(Math.random() * (7 + a * 10));
        for (let i = 0; i < blocks; i++) {
          const ww = (30 + Math.random() * (180 + a * 220)) * dpr;
          const hh = (6 + Math.random() * (45 + a * 60)) * dpr;
          const x = Math.random() * (w - ww);
          const y = Math.random() * (h - hh);
          ctx.fillRect(x, y, ww, hh);
        }

        ctx.globalAlpha = 1;
      }

      ctx.globalCompositeOperation = "source-over";
    }

    requestAnimationFrame(draw);
  }

  // ---------------- wobble filter breathing ----------------
  function animateWobbleFilter() {
    if (prefersReduce) return;

    const turb = document.getElementById("turbulence");
    const disp = document.getElementById("dispMap");
    if (!turb) return;

    let base = 0.012;

    setInterval(() => {
      const a = state.anx;

      // baseFrequency чуть "нервнее" под тревожность
      base += Math.random() * 0.002 - 0.001;
      base = clamp(base, 0.009, 0.016);
      const bf = clamp(base + a * 0.004, 0.009, 0.020);
      turb.setAttribute("baseFrequency", bf.toFixed(4));

      // displacement scale усиливаем под тревожность
      if (disp) {
        const sc = 2 + a * 7 + Math.random() * (0.8 + a * 1.6);
        disp.setAttribute("scale", sc.toFixed(2));
      }
    }, 220);
  }

  // ---------------- LAMP: grab + physics + mood ----------------
  function setupLamp() {
    const lamp = document.querySelector(".lamp");
    const rig = lamp?.querySelector(".lamp__rig");
    const hit = lamp?.querySelector(".lamp__hit");
    const logo = document.getElementById("secretLogo");
    if (!lamp || !rig || !hit || !logo) return;

    // если reduce motion, оставим лампу статичной
    if (prefersReduce) return;

    // маятник
    let theta = Math.random() * 0.10 - 0.05;
    let omega = 0;

    // курсор (для мягкого толкания)
    let px = innerWidth / 2,
      py = 220,
      pvx = 0,
      pvy = 0,
      lastPtT = performance.now();

    // pivot
    let pivotX = 0,
      pivotY = 0;
    const BASE_L = 182;
    // чуть больше амплитуда, чтобы можно было "запустить" маятник
    const MAX = 0.72;
    const OMEGA_MAX = 3.6;

    // геометрия луча (под твой конус)
    const BASE_BEAM_MAX = 520;
    const BEAM_HALF_ANGLE = 0.44; // ~25°
    const BASE_PROXIMITY = 160;
    const BASE_NEAR = 200;
    let lampScale = 1;
    let scaledL = BASE_L;
    let scaledBeamMax = BASE_BEAM_MAX;
    let scaledProximity = BASE_PROXIMITY;
    let scaledNear = BASE_NEAR;
    let lastScaleCheck = 0;

    const readLampScale = () => {
      const raw =
        getComputedStyle(rig).getPropertyValue("--lamp-scale") ||
        getComputedStyle(root).getPropertyValue("--lamp-scale");
      const parsed = parseFloat(raw);
      if (Number.isNaN(parsed) || parsed <= 0) return 1;
      return parsed;
    };

    const updateLampScale = () => {
      lampScale = readLampScale();
      scaledL = BASE_L * lampScale;
      scaledBeamMax = BASE_BEAM_MAX * lampScale;
      scaledProximity = BASE_PROXIMITY * lampScale;
      scaledNear = BASE_NEAR * lampScale;
    };
    updateLampScale();

    function recalcPivot() {
      const r = lamp.getBoundingClientRect();
      pivotX = r.left + r.width / 2;
      pivotY = r.top;
    }
    recalcPivot();
    addEventListener("resize", () => {
      recalcPivot();
      updateLampScale();
    });

    function trackPointer(e) {
      const t = performance.now();
      const dt = Math.max(1, t - lastPtT);

      // скорость мыши (px/ms)
      pvx = clamp((e.clientX - px) / dt, -1.35, 1.35);
      pvy = clamp((e.clientY - py) / dt, -1.35, 1.35);
      px = e.clientX;
      py = e.clientY;
      lastPtT = t;
    }
    addEventListener("pointermove", trackPointer, { passive: true });

    // Угол от вертикали вниз. Плюс: защита от внезапной "инверсии".
    // Знак всегда совпадает с положением курсора относительно центра.
    const angleFromPointer = (x, y) => {
      const dx = x - pivotX;
      const dy = y - pivotY;
      const th = Math.atan2(Math.abs(dx), dy);
      const s = dx === 0 ? 1 : Math.sign(dx);
      return clamp(th * s, -MAX, MAX);
    };

    // grab state
    let grabbing = false;
    let grabLastT = 0;
    let grabLastTheta = 0;

    function ensureHum() {
      if (state.audioStarted) return;

      const AC = window.AudioContext || window.webkitAudioContext;
      if (!AC) return;

      try {
        const ctx = new AC();
        const master = ctx.createGain();
        master.gain.value = 0;
        master.connect(ctx.destination);

        // hum
        const osc = ctx.createOscillator();
        osc.type = "sine";
        osc.frequency.value = 44;
        const humGain = ctx.createGain();
        humGain.gain.value = 0.12;
        osc.connect(humGain);
        humGain.connect(master);

        // noise (very low, фильтруем)
        const len = ctx.sampleRate * 2;
        const buf = ctx.createBuffer(1, len, ctx.sampleRate);
        const data = buf.getChannelData(0);
        for (let i = 0; i < len; i++) data[i] = (Math.random() * 2 - 1) * 0.45;

        const noise = ctx.createBufferSource();
        noise.buffer = buf;
        noise.loop = true;

        const hp = ctx.createBiquadFilter();
        hp.type = "highpass";
        hp.frequency.value = 500;

        const lp = ctx.createBiquadFilter();
        lp.type = "lowpass";
        lp.frequency.value = 2400;

        const noiseGain = ctx.createGain();
        noiseGain.gain.value = 0.03;

        noise.connect(hp);
        hp.connect(lp);
        lp.connect(noiseGain);
        noiseGain.connect(master);

        osc.start();
        noise.start();

        state.audioStarted = true;
        state.audio = { ctx, master, osc, humGain, noiseGain };

        // мягко включаем
        master.gain.setTargetAtTime(0.05, ctx.currentTime, 0.05);
      } catch {
        // молча отвалимся. тревожность пусть будет визуальная.
      }
    }

    function audioTick(t) {
      if (!state.audioStarted || !state.audio) return;
      const { ctx, master, osc, noiseGain } = state.audio;

      // иногда контекст "засыпает" (мобилки), не будем об этом плакать
      if (ctx.state === "suspended") ctx.resume().catch(() => {});

      const a = state.anx;
      const blackout = t < state.blackoutUntil;

      const vol = 0.02 + a * 0.055;
      master.gain.setTargetAtTime(blackout ? vol * 0.35 : vol, ctx.currentTime, 0.06);

      osc.frequency.setTargetAtTime(40 + a * 12, ctx.currentTime, 0.08);
      noiseGain.gain.setTargetAtTime(0.02 + a * 0.03, ctx.currentTime, 0.08);
    }

    hit.addEventListener("pointerenter", () => {
      hit.style.setProperty("--hitVis", "0.26");
    });
    hit.addEventListener("pointerleave", () => {
      if (!grabbing) hit.style.setProperty("--hitVis", "0");
    });

    hit.addEventListener("pointerdown", (e) => {
      // звук только после жеста
      ensureHum();

      grabbing = true;
      document.body.classList.add("lamp-grab");

      grabLastT = performance.now();
      grabLastTheta = theta;

      // маленький "тычок" при хвате
      omega += clamp((e.clientX - pivotX) / 140, -1, 1) * 0.75;
      state.spikeUntil = performance.now() + 260;

      try {
        hit.setPointerCapture(e.pointerId);
      } catch {}
    });

    hit.addEventListener("pointermove", (e) => {
      if (!grabbing) return;

      const now = performance.now();
      const dt = Math.max(1, now - grabLastT);
      const newTheta = angleFromPointer(e.clientX, e.clientY);

      // скорость по углу (рад/с)
      const w = (newTheta - grabLastTheta) / (dt / 1000);
      // не "режем" импульс: пусть можно разогнать маятник
      omega = clamp(lerp(omega, w, 0.85), -OMEGA_MAX, OMEGA_MAX);
      theta = newTheta;

      grabLastT = now;
      grabLastTheta = newTheta;

      // пока держишь, “нервы” сильнее
      state.spikeUntil = now + 120;
    });

    function endGrab() {
      if (!grabbing) return;

      grabbing = false;
      document.body.classList.remove("lamp-grab");
      hit.style.setProperty("--hitVis", "0");

      // дополнительный "флик" при отпускании (чтобы прям качало)
      const vx = pvx * 1000; // px/s
      const vy = pvy * 1000;
      const rx = px - pivotX;
      const ry = py - pivotY;
      const r2 = rx * rx + ry * ry;
      const wPtr = (rx * vy - ry * vx) / (r2 + 1e-6); // рад/с

      omega = clamp(omega + wPtr * 0.28, -OMEGA_MAX, OMEGA_MAX);
      omega = clamp(omega * 1.05, -OMEGA_MAX, OMEGA_MAX);

      state.spikeUntil = performance.now() + 180;
    }

    hit.addEventListener("pointerup", endGrab);
    hit.addEventListener("pointercancel", endGrab);

    let lastT = performance.now();

    function frame(t) {
      const dt = Math.min(0.03, (t - lastT) / 1000);
      lastT = t;

      // физика только когда не держим
      if (!grabbing) {
        // Нормальный маятник, а не резиновая игрушка.
        // Хотим: дал импульс и оно качается, а не умирает через 0.5 секунды.
        const grav = 10.4; // эффективная "g/L"
        const damp = 0.92; // меньше = дольше качается
        const breeze = Math.sin(t * 0.0009) * 0.012 * (0.25 + state.anx * 0.9);

        const bulbX = pivotX + Math.sin(theta) * scaledL;
        const bulbY = pivotY + Math.cos(theta) * scaledL;

        const dxp = px - bulbX;
        const dyp = py - bulbY;
        const dist = Math.hypot(dxp, dyp);

        // мягкое толкание курсором (чуть сильнее, чтобы можно было разогнать)
        if (dist < scaledProximity) {
          const proximity = 1 - dist / scaledProximity;
          const tang = (pvx * -dyp + pvy * dxp) / (dist + 12);
          omega += tang * 0.12 * proximity;
        }

        const acc = -grav * Math.sin(theta) - damp * omega + breeze;
        omega = clamp(omega + acc * dt, -OMEGA_MAX, OMEGA_MAX);
        theta = clamp(theta + omega * dt, -MAX, MAX);

        // если уперлись в предел, чуть гасим, чтобы не дрожало
        if (Math.abs(theta) > MAX - 1e-4) omega *= 0.78;
      }

      // применяем угол
      rig.style.setProperty("--lamp-rot", `${(theta * 180) / Math.PI}deg`);

      // тревожность = близость + скорость + всплески
      const bulbX = pivotX + Math.sin(theta) * scaledL;
      const bulbY = pivotY + Math.cos(theta) * scaledL;
      const distToPointer = Math.hypot(px - bulbX, py - bulbY);
      const near = clamp(1 - distToPointer / scaledNear, 0, 1);

      let spike = 0;
      if (t < state.spikeUntil) {
        spike = clamp((state.spikeUntil - t) / 260, 0, 1);
      }

      const speed = clamp(Math.abs(omega) / OMEGA_MAX, 0, 1);
      const anxTarget = clamp(0.10 + near * 0.60 + speed * 0.85 + spike * 0.75, 0, 1);
      state.anx = lerp(state.anx, anxTarget, 0.06);

      // редкие "морганы" (blackout)
      if (state.blackoutUntil < t && state.anx > 0.68 && Math.random() < 0.012) {
        state.blackoutUntil = t + 70 + Math.random() * 140;
      }
      const blackout = t < state.blackoutUntil;
      document.body.classList.toggle("blackout", blackout);

      // камера: микросдвиг (сглаживаем, чтобы не тошнило)
      const jx = (Math.random() * 2 - 1) * state.anx * 1.6;
      const jy = (Math.random() * 2 - 1) * state.anx * 1.1;
      state.camx = lerp(state.camx, jx, 0.12);
      state.camy = lerp(state.camy, jy, 0.12);
      setRootVar("--camx", `${state.camx.toFixed(2)}px`);
      setRootVar("--camy", `${state.camy.toFixed(2)}px`);

      // фоновые слои
      setRootVar("--moodOp", (0.16 + state.anx * 0.34 + (blackout ? 0.14 : 0)).toFixed(3));
      setRootVar("--moodBlur", blackout ? "0px" : `${(state.anx * 0.6).toFixed(2)}px`);
      setRootVar("--dotsOp", (0.14 + state.anx * 0.26).toFixed(3));
      setRootVar("--noiseOp", (0.18 + state.anx * 0.28).toFixed(3));

      // луч лампы: чуть “дышит” и дергается по тревожности
      const beamOp = 0.46 + state.anx * 0.30 + (blackout ? -0.30 : 0);
      const beamBlur = 6.5 + state.anx * 12.0 + (blackout ? -3.5 : 0);
      const beamGrain = 0.08 + state.anx * 0.30 + (blackout ? 0.16 : 0);
      const beamSX = 1 + (state.anx * 0.02) + (Math.sin(t * 0.006) * 0.01 * state.anx);
      const beamSY = 1 + (state.anx * 0.018) + (Math.cos(t * 0.005) * 0.01 * state.anx);

      rig.style.setProperty("--beamOp", clamp(beamOp, 0.05, 0.82).toFixed(3));
      rig.style.setProperty("--beamBlur", `${clamp(beamBlur, 2.5, 22).toFixed(1)}px`);
      rig.style.setProperty("--beamGrain", clamp(beamGrain, 0, 0.65).toFixed(3));
      rig.style.setProperty("--beamSX", beamSX.toFixed(3));
      rig.style.setProperty("--beamSY", beamSY.toFixed(3));

      rig.style.setProperty("--bulbBright", (1 + state.anx * 0.12 + spike * 0.12).toFixed(3));
      rig.style.setProperty("--bulbHalo", (0.20 + state.anx * 0.20).toFixed(3));

      // ----- reveal logo only when beam hits -----
      const apexX = pivotX + Math.sin(theta) * scaledL;
      const apexY = pivotY + Math.cos(theta) * scaledL + 16;

      const dirX = Math.sin(theta);
      const dirY = Math.cos(theta);

      const r = logo.getBoundingClientRect();
      const lx = r.left + r.width / 2;
      const ly = r.top + r.height / 2;

      const vx = lx - apexX;
      const vy = ly - apexY;

      const along = vx * dirX + vy * dirY;
      const perp = Math.abs(vx * dirY - vy * dirX);

      let vis = 0;

      if (along > 0 && along < scaledBeamMax) {
        const maxPerp = along * Math.tan(BEAM_HALF_ANGLE);
        const edge = 1 - perp / (maxPerp + 1e-6);

        const inside = smoothstep(0.0, 1.0, edge);
        const fadeIn = smoothstep(10, 70, along);
        const fadeOut = 1 - smoothstep(scaledBeamMax - 100, scaledBeamMax, along);

        // нервное мерцание зависит от тревожности
        const flicker =
          (0.90 + 0.10 * Math.sin(t * (0.010 + state.anx * 0.010))) *
          (1 - (blackout ? 0.92 : 0));

        vis = inside * fadeIn * fadeOut * flicker;
      }

      logo.style.setProperty("--logoVis", clamp(vis, 0, 1).toFixed(3));

      // микро-джиттер у лого (только если оно реально видно)
      if (vis > 0.05 && state.anx > 0.2 && !blackout) {
        const j = state.anx * vis * 1.8;
        logo.style.setProperty("--logoJx", `${((Math.random() * 2 - 1) * j).toFixed(2)}px`);
        logo.style.setProperty("--logoJy", `${((Math.random() * 2 - 1) * j).toFixed(2)}px`);
      } else {
        logo.style.setProperty("--logoJx", "0px");
        logo.style.setProperty("--logoJy", "0px");
      }

      audioTick(t);
      if (t - lastScaleCheck > 500) {
        updateLampScale();
        lastScaleCheck = t;
      }
      requestAnimationFrame(frame);
    }

    requestAnimationFrame(frame);
  }

  // ---------------- BOOT ----------------
  function boot() {
    const safe = (name, fn) => {
      try {
        fn();
      } catch (e) {
        console.error(`[SEROTONYL] ${name} crashed:`, e);
      }
    };

    safe("enhanceSecretLogoText", enhanceSecretLogoText);
    safe("setupRippleToggle", setupRippleToggle);
    safe("setupDoors", setupDoors);
    safe("setupDustCanvas", setupDustCanvas);
    safe("animateWobbleFilter", animateWobbleFilter);
    safe("setupLamp", setupLamp);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})();
