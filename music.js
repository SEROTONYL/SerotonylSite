/*
  SEROTONYL / MUSIC (panic viz patch)
  Fixes:
  - reels no longer "fall" (JS doesn't overwrite translate transform)
  - INTENSITY slider actually controls the madness
  - background reacts hard to audio (bands: bass/mid/high)
  - canvas selector supports both #react and legacy #reactCanvas
*/

(() => {
  "use strict";

  const TRACKS = [
    { title: "D1g1tal rain in sunny p4rk", meta: "SEROTONYL · 160 bpm", src: "./audio/preview1.mp3" },
    { title: "Memories",     meta: "SEROTONYL · 116 bpm", src: "./audio/preview2.mp3" },
    { title: "YOUR MAJESTY",      meta: "SEROTONYL · 125 bpm", src: "./audio/preview3.mp3" },
  ];

  const $ = (sel) => document.querySelector(sel);
  const $$ = (sel) => Array.from(document.querySelectorAll(sel));
  const clamp = (v, a, b) => Math.min(b, Math.max(a, v));

  const fmtTime = (sec) => {
    if (!Number.isFinite(sec) || sec <= 0) return "0:00";
    const m = Math.floor(sec / 60);
    const s = Math.floor(sec % 60);
    return `${m}:${String(s).padStart(2, "0")}`;
  };

  window.addEventListener("DOMContentLoaded", () => {
    // show page if you still keep body opacity gate
    requestAnimationFrame(() => document.body.classList.add("is-ready"));

    const audio = /** @type {HTMLAudioElement|null} */ ($("#audio"));
    const deck = $("#deck");

    const playBtn = /** @type {HTMLButtonElement|null} */ ($("#playBtn"));
    const stopBtn = /** @type {HTMLButtonElement|null} */ ($("#stopBtn"));
    const prevBtn = /** @type {HTMLButtonElement|null} */ ($("#prevBtn"));
    const nextBtn = /** @type {HTMLButtonElement|null} */ ($("#nextBtn"));

    const seek = /** @type {HTMLInputElement|null} */ ($("#seek"));
    const curTime = $("#curTime");
    const durTime = $("#durTime");
    const trackTitle = $("#trackTitle");
    const trackMeta = $("#trackMeta");

    const reelL = $("#reelL");
    const reelR = $("#reelR");

    const rackCards = $$("#rackGrid .tapeCard");

    const copyTpl = /** @type {HTMLButtonElement|null} */ ($("#copyTpl"));
    const tpl = /** @type {HTMLTextAreaElement|null} */ ($("#tpl"));
    const copyStatus = $("#copyStatus");
    const copyNote = $("#copyNote");

    // INTENSITY
    const intensityEl = /** @type {HTMLInputElement|null} */ ($("#intensity"));
    let intensity = intensityEl ? clamp(Number(intensityEl.value) / 100, 0, 1) : 0.65;
    intensityEl?.addEventListener("input", () => {
      intensity = clamp(Number(intensityEl.value) / 100, 0, 1);
    });

    // canvas
    const canvas = /** @type {HTMLCanvasElement|null} */ ($("#react") || $("#reactCanvas"));
    const ctx = canvas ? canvas.getContext("2d", { alpha: true }) : null;

    if (!audio) {
      console.error("[music] Не найден <audio id='audio'>");
      return;
    }

    let index = 0;
    let raf = 0;
    let seeking = false;
    let reelPhase = 0;

    // WebAudio
    const canWebAudio = location.protocol !== "file:";
    let AC = null;
    let analyser = null;
    let data = null;
    let source = null;

    const ensureAnalyser = () => {
      if (!canWebAudio) return;
      if (analyser) return;

      const ACtor = window.AudioContext || window["webkitAudioContext"];
      if (!ACtor) return;

      try {
        AC = new ACtor();
        analyser = AC.createAnalyser();
        analyser.fftSize = 2048;
        data = new Uint8Array(analyser.frequencyBinCount);

        source = AC.createMediaElementSource(audio);
        source.connect(analyser);
        analyser.connect(AC.destination);
      } catch (e) {
        AC = null; analyser = null; data = null; source = null;
        console.warn("[music] WebAudio analyser недоступен:", e);
      }
    };

    const resizeCanvas = () => {
      if (!canvas || !ctx) return;
      const dpr = Math.min(2, window.devicePixelRatio || 1);
      const w = Math.floor(window.innerWidth * dpr);
      const h = Math.floor(window.innerHeight * dpr);
      canvas.width = w;
      canvas.height = h;
      canvas.style.width = "100%";
      canvas.style.height = "100%";
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    };

    // --- UI ---
    const setActiveCard = () => {
      rackCards.forEach((b, i) => b.classList.toggle("is-active", i === index));
    };

    const setDeckText = () => {
      const t = TRACKS[index];
      if (trackTitle) trackTitle.textContent = t.title;
      if (trackMeta) trackMeta.textContent = t.meta;
    };

    const setTrack = (i, opts = { autoplay: false }) => {
      index = (i + TRACKS.length) % TRACKS.length;
      setActiveCard();

      if (deck) {
        deck.classList.add("is-switching");
        setTimeout(() => deck.classList.remove("is-switching"), 240);
      }

      setDeckText();

      audio.src = TRACKS[index].src;
      audio.load();

      if (seek) seek.value = "0";
      if (curTime) curTime.textContent = "0:00";
      if (durTime) durTime.textContent = "0:00";

      if (opts.autoplay) {
        play().catch(() => {});
      }
    };

    const play = async () => {
      ensureAnalyser();
      if (AC && AC.state === "suspended") {
        try { await AC.resume(); } catch (_) {}
      }
      try {
        await audio.play();
        document.body.classList.add("is-playing");
        if (playBtn) playBtn.textContent = "PAUSE";
        startLoop();
      } catch (e) {
        if (copyNote) copyNote.textContent = "Не смог запустить аудио. Проверь путь к файлу и DevTools → Console.";
        throw e;
      }
    };

    const pause = () => {
      audio.pause();
      document.body.classList.remove("is-playing");
      if (playBtn) playBtn.textContent = "PLAY";
    };

    const stopAll = () => {
      audio.pause();
      audio.currentTime = 0;
      document.body.classList.remove("is-playing");
      if (playBtn) playBtn.textContent = "PLAY";
    };

    const togglePlay = () => {
      if (audio.paused) void play();
      else pause();
    };

    // --- audio bands ---
    const bandAvg = (arr, from, to) => {
      const a = clamp(from | 0, 0, arr.length);
      const b = clamp(to | 0, 0, arr.length);
      if (b <= a) return 0;
      let s = 0;
      for (let i = a; i < b; i++) s += arr[i];
      return (s / (b - a)) / 255;
    };

    const getBands = () => {
      if (analyser && data) {
        analyser.getByteFrequencyData(data);
        // индексы не в Гц, но для эффекта хватает: низ/середина/верх
        const bass = bandAvg(data, 2, 24);
        const mid  = bandAvg(data, 24, 96);
        const high = bandAvg(data, 96, 220);
        return { bass, mid, high };
      }
      // фейк для file://
      const t = audio && !audio.paused ? audio.currentTime : 0;
      return {
        bass: audio && !audio.paused ? (0.25 + 0.25 * Math.abs(Math.sin(t * 2.3))) : 0,
        mid:  audio && !audio.paused ? (0.15 + 0.20 * Math.abs(Math.sin(t * 3.7))) : 0,
        high: audio && !audio.paused ? (0.10 + 0.18 * Math.abs(Math.sin(t * 6.1))) : 0,
      };
    };

    // --- VISUALS ---
    const drawMadness = (bass, mid, high) => {
      const t = performance.now() * 0.001;
      const energy = clamp((bass * 1.0 + mid * 0.55 + high * 0.35) * (0.25 + intensity * 1.25), 0, 1);

      // reels: rotate via CSS var, do NOT overwrite translate
      if (!audio.paused) reelPhase += 0.12 + energy * 0.9;
      if (reelL) reelL.style.setProperty("--spin", `${reelPhase * 55}deg`);
      if (reelR) reelR.style.setProperty("--spin", `${-reelPhase * 60}deg`);

      // shake & chroma
      const shake = (bass ** 1.4) * 18 * intensity;
      const sx = Math.sin(t * 14.0) * shake;
      const sy = Math.cos(t * 17.0) * shake;
      document.documentElement.style.setProperty("--sx", `${sx.toFixed(2)}px`);
      document.documentElement.style.setProperty("--sy", `${sy.toFixed(2)}px`);
      document.documentElement.style.setProperty("--beat", String(clamp(bass * (0.6 + intensity), 0, 1)));
      document.documentElement.style.setProperty("--chrom", String(clamp(high * (0.5 + intensity * 2.2), 0, 3)));
      document.documentElement.style.setProperty("--bgShift", String((Math.sin(t * 0.8) * 18 + bass * 60) * intensity));

      const alpha = clamp(0.10 + energy * 0.55 * (0.35 + intensity), 0, 0.9);
      document.documentElement.style.setProperty("--reactAlpha", String(alpha));
      document.documentElement.style.setProperty("--pulse", String(energy));

      if (!canvas || !ctx) return;
      const w = window.innerWidth;
      const h = window.innerHeight;

      // trail fade
      ctx.globalCompositeOperation = "source-over";
      ctx.fillStyle = `rgba(0,0,0,${0.10 + (1 - intensity) * 0.10})`;
      ctx.fillRect(0, 0, w, h);

      // additive bloom
      ctx.globalCompositeOperation = "lighter";

      const cx = w * (0.5 + Math.sin(t * 0.9) * 0.08 * intensity);
      const cy = h * (0.45 + Math.cos(t * 0.7) * 0.08 * intensity);
      const R = Math.max(w, h) * (0.55 + bass * 0.55 * intensity);

      const g = ctx.createRadialGradient(cx, cy, 0, cx, cy, R);
      g.addColorStop(0, `rgba(184,76,255,${0.18 + high * 0.20 * intensity})`);
      g.addColorStop(0.35, `rgba(55,255,179,${0.10 + mid * 0.18 * intensity})`);
      g.addColorStop(1, "rgba(0,0,0,0)");
      ctx.fillStyle = g;
      ctx.beginPath();
      ctx.arc(cx, cy, R, 0, Math.PI * 2);
      ctx.fill();

      // shockwave rings on bass hits
      const ring = (bass ** 1.8) * 140 * intensity;
      if (ring > 10) {
        ctx.strokeStyle = `rgba(255,47,146,${0.10 + bass * 0.25 * intensity})`;
        ctx.lineWidth = 2 + bass * 6 * intensity;
        ctx.beginPath();
        ctx.arc(cx, cy, ring, 0, Math.PI * 2);
        ctx.stroke();
      }

      // scanline glitches
      if (energy > 0.35 && Math.random() < (0.10 + intensity * 0.18)) {
        const sy0 = Math.floor(Math.random() * h);
        const sh = Math.floor(6 + Math.random() * 20);
        const dx = (Math.random() - 0.5) * (40 + 240 * intensity) * (0.25 + bass);
        // self-copy slice (cheap glitch)
        try {
          ctx.globalCompositeOperation = "source-over";
          ctx.drawImage(canvas, 0, sy0, w, sh, dx, sy0 + (Math.random() - 0.5) * 6, w, sh);
        } catch (_) {}
      }

      ctx.globalCompositeOperation = "source-over";
    };

    const tick = () => {
      if (curTime) curTime.textContent = fmtTime(audio.currentTime);
      if (durTime) durTime.textContent = fmtTime(audio.duration);

      if (seek && !seeking && Number.isFinite(audio.duration) && audio.duration > 0) {
        const v = Math.round((audio.currentTime / audio.duration) * 1000);
        seek.value = String(clamp(v, 0, 1000));
      }

      const { bass, mid, high } = getBands();
      drawMadness(bass, mid, high);
      raf = requestAnimationFrame(tick);
    };

    const startLoop = () => {
      if (raf) return;
      raf = requestAnimationFrame(tick);
    };

    const stopLoop = () => {
      if (!raf) return;
      cancelAnimationFrame(raf);
      raf = 0;
    };

    // --- events ---
    playBtn?.addEventListener("click", togglePlay);
    stopBtn?.addEventListener("click", () => { stopAll(); stopLoop(); });
    prevBtn?.addEventListener("click", () => setTrack(index - 1, { autoplay: !audio.paused }));
    nextBtn?.addEventListener("click", () => setTrack(index + 1, { autoplay: !audio.paused }));

    rackCards.forEach((btn) => {
      btn.addEventListener("click", () => {
        const i = Number(btn.dataset.i);
        if (Number.isFinite(i)) setTrack(i, { autoplay: !audio.paused });
      });
    });

    seek?.addEventListener("pointerdown", () => { seeking = true; });
    seek?.addEventListener("pointerup", () => { seeking = false; });
    seek?.addEventListener("input", () => {
      if (!seek || !Number.isFinite(audio.duration) || audio.duration <= 0) return;
      const v = Number(seek.value);
      audio.currentTime = clamp((v / 1000) * audio.duration, 0, audio.duration);
    });

    audio.addEventListener("ended", () => {
      document.body.classList.remove("is-playing");
      if (playBtn) playBtn.textContent = "PLAY";
      stopLoop();
    });

    audio.addEventListener("play", startLoop);
    audio.addEventListener("pause", () => {
      document.body.classList.remove("is-playing");
      if (playBtn) playBtn.textContent = "PLAY";
    });

    copyTpl?.addEventListener("click", async () => {
      if (!tpl) return;
      const text = tpl.value;
      try {
        await navigator.clipboard.writeText(text);
        if (copyStatus) copyStatus.textContent = "скопировано";
        setTimeout(() => { if (copyStatus) copyStatus.textContent = ""; }, 1200);
      } catch (e) {
        tpl.focus();
        tpl.select();
        const ok = document.execCommand("copy");
        if (copyStatus) copyStatus.textContent = ok ? "скопировано" : "не вышло";
        setTimeout(() => { if (copyStatus) copyStatus.textContent = ""; }, 1200);
      }
    });

    // resize
    resizeCanvas();
    window.addEventListener("resize", resizeCanvas);

    // init
    setDeckText();
    setActiveCard();
    setTrack(0, { autoplay: false });


  });
})();
