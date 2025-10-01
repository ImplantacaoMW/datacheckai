document.addEventListener("DOMContentLoaded", function() {
    if (!document.querySelector('.barra-animada')) return;

    function animaBarra(element, targetPct, duration = 4200, delay = 400) {
        const label = element.querySelector('.barra-label');
        let start = null;
        let pctFinal = Math.max(0, Math.min(targetPct, 100)); // clamp
        setTimeout(() => {
            element.style.transition = `width ${duration}ms cubic-bezier(.55,1.6,.15,1)`;
            // anima barra + número
            function step(timestamp) {
                if (!start) start = timestamp;
                let progress = Math.min((timestamp - start) / duration, 1);
                let current = Math.floor(progress * pctFinal);
                element.style.width = current + "%";
                if (label) label.textContent = current + "%";
                if (progress < 1) {
                    requestAnimationFrame(step);
                } else {
                    element.style.width = pctFinal + "%";
                    if (label) label.textContent = pctFinal + "%";
                }
            }
            requestAnimationFrame(step);
        }, delay);
    }

    document.querySelectorAll('.barra-animada').forEach(function(barraContainer) {
        const barraValidos = barraContainer.querySelector('.barra-animada-validos');
        const barraInvalidos = barraContainer.querySelector('.barra-animada-invalidos');
        if (barraValidos) {
            let pct = 0;
            const label = barraValidos.querySelector('.barra-label');
            if (label) {
                const match = label.textContent.match(/(\d+)%/);
                if (match) pct = parseInt(match[1]);
                label.textContent = "0%";
            }
            animaBarra(barraValidos, pct, 4200, 400);
        }
        if (barraInvalidos) {
            let pct = 0;
            const label = barraInvalidos.querySelector('.barra-label');
            if (label) {
                const match = label.textContent.match(/(\d+)%/);
                if (match) pct = parseInt(match[1]);
                label.textContent = "0%";
            }
            animaBarra(barraInvalidos, pct, 4200, 400);
        }
    });
});