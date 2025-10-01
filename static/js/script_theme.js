document.addEventListener('DOMContentLoaded', () => {
    
    // BOTÃO TOGGLE THEME
    const btnToggleTheme = document.getElementById('btnToggleTheme');

    // Cria o ícone inicial (lâmpada)
    if (btnToggleTheme) {
        const icon = document.createElement('i');
        icon.className = 'fas fa-lightbulb';  // lâmpada (tema claro)
        btnToggleTheme.appendChild(icon);

        // Aplica tema salvo
        if (localStorage.getItem('theme') === 'dark') {
            document.body.classList.add('theme-dark');
            icon.className = 'fas fa-moon'; // lua no tema escuro
        }

        btnToggleTheme.addEventListener('click', () => {
            const isDark = document.body.classList.toggle('theme-dark');
            if (isDark) {
                localStorage.setItem('theme', 'dark');
                icon.className = 'fas fa-moon';
            } else {
                localStorage.removeItem('theme');
                icon.className = 'fas fa-lightbulb';
            }
        });
    }
});