document.addEventListener('DOMContentLoaded', () => {
    // --- Theme Toggle (deixe apenas UM desses scripts no projeto!) ---
    const btnToggleTheme = document.getElementById('btnToggleTheme');
    if (btnToggleTheme) {
        // Ícone criado pelo script_theme.js, se já incluído. Se não, crie aqui:
        if (!btnToggleTheme.querySelector('i')) {
            const icon = document.createElement('i');
            icon.className = 'fas fa-lightbulb';
            btnToggleTheme.appendChild(icon);
        }
        // Controle tema
        const icon = btnToggleTheme.querySelector('i');
        if (localStorage.getItem('theme') === 'dark') {
            document.body.classList.add('theme-dark');
            if (icon) icon.className = 'fas fa-moon';
        }
        btnToggleTheme.addEventListener('click', () => {
            const isDark = document.body.classList.toggle('theme-dark');
            if (isDark) {
                localStorage.setItem('theme', 'dark');
                if (icon) icon.className = 'fas fa-moon';
            } else {
                localStorage.removeItem('theme');
                if (icon) icon.className = 'fas fa-lightbulb';
            }
        });
    }

    // --- Mostra resultsSection com botão de cópia após análise ---
    const resultsSection = document.getElementById('resultsSection');
    if (resultsSection && document.querySelector('.resultado-card-analise')) {
        resultsSection.style.display = 'block';
    }

    // --- Custom File Input and List Display + Limpeza ---
    const fileInput = document.getElementById('fileInput');
    const btnFileSelect = document.getElementById('btnFileSelect');
    const fileList = document.getElementById('fileList');
    if (btnFileSelect && fileInput && fileList) {
        btnFileSelect.addEventListener('click', () => {
            fetch('/limpar_uploads', {method: 'POST'})
                .then(() => {
                    fileInput.value = "";
                    fileList.innerHTML = "";
                    fileInput.click();
                });
        });
        fileInput.addEventListener('change', () => {
            fileList.innerHTML = '';
            const files = fileInput.files;
            if (files.length > 0) {
                for (let i = 0; i < files.length; i++) {
                    const fileItem = document.createElement('div');
                    fileItem.className = 'file-item';
                    fileItem.textContent = files[i].name;
                    fileList.appendChild(fileItem);
                }
            }
        });
    }

    // --- Loading Overlay for Upload Form ---
    const uploadForm = document.getElementById('uploadForm');
    const loadingOverlay = document.getElementById('loadingOverlay');
    if (uploadForm && loadingOverlay) {
        uploadForm.addEventListener('submit', function() {
            loadingOverlay.style.display = 'flex';
        });
    }

    // --- Form Mapeamento Submission and Loading Overlay ---
    const formMapear = document.getElementById('formMapear');
    if (formMapear && loadingOverlay) {
        formMapear.addEventListener('submit', function(e) {
            let isValid = true;
            formMapear.querySelectorAll('select[required]').forEach(field => {
                if (!field.value) {
                    isValid = false;
                    field.style.borderColor = '#dc3545';
                } else {
                    field.style.borderColor = '';
                }
            });
            if (!isValid) {
                e.preventDefault();
                alert('Por favor, preencha todos os campos obrigatórios!');
            } else {
                loadingOverlay.style.display = 'flex';
            }
        });
    }

    // --- UX for mapping tables and column selection ---
    document.querySelectorAll('.tabela-mapear').forEach(tabela => {
        const headers = tabela.querySelectorAll('thead th');
        headers.forEach((th, index) => {
            th.addEventListener('click', () => {
                headers.forEach(h => h.classList.remove('col-selected'));
                tabela.querySelectorAll('tbody tr').forEach(row => {
                    row.querySelectorAll('td').forEach(td => td.classList.remove('col-selected-cell'));
                });
                th.classList.add('col-selected');
                tabela.querySelectorAll('tbody tr').forEach(row => {
                    const cell = row.children[index];
                    if (cell) cell.classList.add('col-selected-cell');
                });
            });
        });
    });

    // --- Highlight rows on hover for mapping tables ---
    document.querySelectorAll('.tabela-mapear tbody tr').forEach(row => {
        row.addEventListener('mouseenter', () => row.style.backgroundColor = '#f5f9fa');
        row.addEventListener('mouseleave', () => row.style.backgroundColor = '');
    });

    // --- "Load More" functionality for sample data tables ---
    document.querySelectorAll('.btn-load-more').forEach(btn => {
        btn.addEventListener('click', function(e) {
            e.preventDefault();
            const container = btn.closest('.tabela-wrapper-scroll');
            const table = container.querySelector('table');
            const nomeArquivo = btn.dataset.filename;
            const offset = table.querySelectorAll('tbody tr').length;
            const limit = 20;

            btn.disabled = true;
            btn.textContent = 'Carregando...';

            fetch('/get_amostra', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/x-www-form-urlencoded'
                },
                body: `nome_arquivo=${encodeURIComponent(nomeArquivo)}&offset=${offset}&limit=${limit}`
            })
            .then(res => res.json())
            .then(data => {
                if (!data.ok || !data.amostra || data.amostra.length === 0) {
                    btn.textContent = 'Todos os dados carregados';
                    btn.disabled = true;
                    return;
                }

                let tbody = table.querySelector('tbody');
                data.amostra.forEach(row => {
                    let tr = document.createElement('tr');
                    data.colunas.forEach(col => {
                        let td = document.createElement('td');
                        td.textContent = row[col] ?? '';
                        tr.appendChild(td);
                    });
                    tbody.appendChild(tr);
                });

                btn.textContent = '+ Linhas';
                btn.disabled = false;

                if (data.amostra.length < limit) {
                    btn.textContent = 'Todos os dados carregados';
                    btn.disabled = true;
                }

                container.scrollTo({
                    top: container.scrollHeight,
                    behavior: 'smooth'
                });
            })
            .catch(error => {
                console.error('Erro ao carregar mais linhas:', error);
                btn.textContent = 'Erro ao carregar. Tentar novamente';
                btn.disabled = false;
                alert('Ocorreu um erro ao carregar mais dados. Por favor, tente novamente.');
            });
        });
    });

    // --- Copiar Resultado para E-mail ---
    const copyBtn = document.getElementById('copyResultBtn');
    if (copyBtn) {
        copyBtn.addEventListener('click', function() {
            const cards = document.querySelectorAll('.resultado-card-analise');
            if (!cards.length) return;

            let fullReport = "";

            cards.forEach(card => {
                // Nome do arquivo
                let nomeArquivo = card.getAttribute('data-nome-arquivo') || '-';

                let reportText = "=== RELATÓRIO DE ANÁLISE - CADASTRO DE MERCADORIAS ===\n";
                reportText += "ARQUIVO ANALISADO: " + nomeArquivo + "\n\n";

                // Totais
                const total = card.querySelector('.stat-total') ? card.querySelector('.stat-total').textContent.trim() : '-';
                const validos = card.querySelector('.stat-validos') ? card.querySelector('.stat-validos').textContent.trim() : '-';
                const invalidos = card.querySelector('.stat-invalidos') ? card.querySelector('.stat-invalidos').textContent.trim() : '-';

                reportText += "RESUMO GERAL:\n";
                reportText += `• Total de registros processados: ${total}\n`;
                reportText += `• Registros válidos: ${validos}\n`;
                reportText += `• Registros com erro: ${invalidos}\n`;
                let percent = 0;
                if (!isNaN(Number(validos)) && !isNaN(Number(total)) && Number(total) > 0) {
                    percent = Math.round((Number(validos) / Number(total)) * 100);
                }
                reportText += `• Taxa de sucesso: ${percent}%\n`;

                // Inconsistências
                const incBox = card.querySelector('.inc-box');
                if (incBox) {
                    reportText += "\nINCONSISTÊNCIAS ENCONTRADAS:\n";

                    // Grupos
                    const grupos = incBox.querySelectorAll('.grupo-inc');
                    grupos.forEach(grupo => {
                        const grupoTitulo = grupo.querySelector('.grupo-titulo');
                        if (grupoTitulo) {
                            reportText += `\n${grupoTitulo.textContent.trim().toUpperCase()}:\n\n`;
                        }

                        // Cada campo/motivo
                        const campos = grupo.querySelectorAll('.inc-titulo');
                        campos.forEach((titulo) => {
                            const label = titulo.querySelector('b') ? titulo.querySelector('b').textContent.trim() : '';
                            let mensagem = titulo.textContent.replace(label, '').replace('—', '').trim();
                            mensagem = mensagem.replace(/\s+/g, ' ').trim();

                            // Campo e motivo juntos na mesma linha
                            reportText += `   - ${label}*: ${mensagem}`;

                            // Amostras (se houver)
                            let amostra = titulo.nextElementSibling;
                            if (amostra && amostra.classList.contains('amostra-lista')) {
                                const exemplos = amostra.querySelectorAll('li');
                                exemplos.forEach(li => {
                                    reportText += `\n      - ${li.textContent.trim()}`;
                                });
                            }

                            // Sempre uma linha em branco entre motivos
                            reportText += '\n\n';
                        });
                    });
                } else {
                    reportText += "\nNenhuma inconsistência encontrada.\n";
                }

                reportText += "=== FIM DO RELATÓRIO ===\n";
                fullReport += reportText + "\n";
            });

            navigator.clipboard.writeText(fullReport)
                .then(() => showSnackbar('Relatório copiado para área de transferência!'))
                .catch(() => showSnackbar('Falha ao copiar relatório.'));
        });
    }

    // Snackbar simples (caso não exista)
    function showSnackbar(message, duration = 3200) {
        let snackbar = document.getElementById('snackbar');
        if (!snackbar) {
            snackbar = document.createElement('div');
            snackbar.id = 'snackbar';
            document.body.appendChild(snackbar);
        }
        snackbar.textContent = message;
        snackbar.className = 'show';
        setTimeout(() => {
            snackbar.className = snackbar.className.replace('show', '');
        }, duration);
    }

    // --- Snackbar Implementation ---
    function showSnackbar(message, duration = 3200) {
        let snackbar = document.getElementById('snackbar');
        if (!snackbar) {
            snackbar = document.createElement('div');
            snackbar.id = 'snackbar';
            document.body.appendChild(snackbar);
        }
        snackbar.innerHTML = message + '<div class="snackbar-progress"></div>';
        snackbar.className = 'show';

        // Progress bar animation
        const progress = snackbar.querySelector('.snackbar-progress');
        if (progress) {
            progress.style.transition = 'none';
            progress.style.transform = 'scaleX(1)';
            // trigger reflow
            progress.offsetWidth;
            progress.style.transition = `transform ${duration}ms linear`;
            progress.style.transform = 'scaleX(0)';
        }

        setTimeout(() => {
            snackbar.className = snackbar.className.replace('show', '');
        }, duration);
    }

    // --- Loading Overlay  ---
    var extraMsg = document.getElementById("loadingExtraMsg");
    var extraMsgTwo = document.getElementById("loadingExtraMsgTwo");
    var overlay = document.getElementById("loadingOverlay");

    if (overlay) {
        var triggerOverlay = function() {
            overlay.style.display = "flex";
            if (extraMsg) extraMsg.style.display = 'none';
            if (extraMsgTwo) extraMsgTwo.style.display = 'none';

            timer1 = setTimeout(function () {
                if (overlay.style.display !== "none" && extraMsg) {
                    extraMsg.style.display = 'block';
                }
            }, 15000);

            timer2 = setTimeout(function () {
                if (overlay.style.display !== "none" && extraMsgTwo) {
                    extraMsgTwo.style.display = 'block';
                }
            }, 50000);
        };

        var forms = [document.getElementById("uploadForm"), document.getElementById("formMapear")];
        forms.forEach(function (form) {
            if (form) {
                form.addEventListener("submit", function () {
                    triggerOverlay();
                });
            }
        });
    }

    // --- Barra animada resultado (animação suave) ---
    document.querySelectorAll('.barra-animada-validos, .barra-animada-invalidos').forEach(function(bar){
        var finalWidth = bar.style.width;
        bar.style.width = '0%';
        setTimeout(function(){
            bar.style.transition = "width 1.1s cubic-bezier(.48,.72,.21,1.13)";
            bar.style.width = finalWidth;
        }, 120);
    });
});