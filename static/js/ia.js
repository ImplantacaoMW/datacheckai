document.addEventListener('DOMContentLoaded', function() {
    const modalConfirmacao = new bootstrap.Modal(document.getElementById('modalConfirmacao'));
    const modalMsg = document.getElementById('modalMsg');
    const modalBtnConfirmar = document.getElementById('modalBtnConfirmar');
    const snackbar = document.getElementById('snackbar');
    let dadosParaExcluir = {};

    // --- Lógica de Exclusão ---

    function configurarExclusao(e) {
        const target = e.target.closest('.btn-excluir-campo, .btn-excluir-valor');
        if (!target) return;

        const layout = target.dataset.layout;
        const campo = target.dataset.campo;
        const valor = target.dataset.valor;

        dadosParaExcluir = { layout, campo, valor, acao: valor ? 'delvalor' : 'delcampo', element: target };

        if (valor) {
            modalMsg.textContent = `Tem certeza que deseja excluir o valor "${valor}" do campo "${campo}"?`;
        } else {
            modalMsg.textContent = `Tem certeza que deseja limpar TODAS as amostras do campo "${campo}"? Esta ação não pode ser desfeita.`;
        }
        modalConfirmacao.show();
    }

    document.body.addEventListener('click', configurarExclusao);

    modalBtnConfirmar.addEventListener('click', function() {
        const { layout, campo, valor, acao, element } = dadosParaExcluir;
        const token = new URLSearchParams(window.location.search).get('token');

        fetch('/history_ia/delete', {
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body: new URLSearchParams({
                token: token,
                layout: layout,
                campo: campo,
                valor: valor || '',
                acao: acao
            })
        })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                if (acao === 'delvalor') {
                    element.closest('li').remove();
                } else {
                    const container = element.closest('.campo-container');
                    container.querySelector('.lista-amostras').innerHTML = '';
                    container.querySelector('.badge').textContent = '0';
                }
                mostrarSnackbar(data.mensagem, 'success');
            } else {
                mostrarSnackbar(data.mensagem || 'Erro ao excluir.', 'error');
            }
        })
        .catch(() => mostrarSnackbar('Erro de conexão.', 'error'))
        .finally(() => modalConfirmacao.hide());
    });

    // --- Lógica de Busca ---

    function executarBusca(input) {
        const layout = input.dataset.layout;
        const campo = input.dataset.campo;
        const termo = input.value;
        const token = new URLSearchParams(window.location.search).get('token');
        const lista = input.closest('.campo-container').querySelector('.lista-amostras');

        fetch('/history_ia/busca_amostras', {
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body: new URLSearchParams({
                token: token,
                layout: layout,
                campo: campo,
                termo: termo
            })
        })
        .then(response => response.json())
        .then(data => {
            lista.innerHTML = '';
            if (data.amostras && data.amostras.length > 0) {
                data.amostras.forEach(amostra => {
                    const li = document.createElement('li');
                    li.className = 'list-group-item d-flex justify-content-between align-items-center';
                    li.innerHTML = `
                        <span class="valor-amostra">${amostra}</span>
                        <button class="btn btn-sm btn-outline-danger btn-excluir-valor" data-layout="${layout}" data-campo="${campo}" data-valor="${amostra}">
                            <i class="bi bi-x-lg"></i>
                        </button>
                    `;
                    lista.appendChild(li);
                });
            } else {
                lista.innerHTML = '<li class="list-group-item">Nenhuma amostra encontrada.</li>';
            }
        })
        .catch(() => {
            lista.innerHTML = '<li class="list-group-item text-danger">Erro ao buscar.</li>';
        });
    }

    document.querySelectorAll('.btn-busca').forEach(btn => {
        btn.addEventListener('click', function() {
            const input = this.previousElementSibling;
            executarBusca(input);
        });
    });

    document.querySelectorAll('.input-busca').forEach(input => {
        input.addEventListener('keyup', function(e) {
            if (e.key === 'Enter') {
                executarBusca(input);
            }
        });
    });

    // --- Snackbar e Mensagens ---
    function mostrarSnackbar(mensagem, tipo = 'info') {
        snackbar.textContent = mensagem;
        snackbar.className = 'snackbar show';
        if (tipo === 'success') {
            snackbar.style.backgroundColor = '#28a745';
        } else if (tipo === 'error') {
            snackbar.style.backgroundColor = '#dc3545';
        } else {
            snackbar.style.backgroundColor = '#17a2b8';
        }
        setTimeout(() => { snackbar.className = snackbar.className.replace('show', ''); }, 3000);
    }

    const mensagemBackend = document.getElementById('mensagem-backend').dataset.mensagem;
    if (mensagemBackend) {
        mostrarSnackbar(mensagemBackend, 'info');
    }
});