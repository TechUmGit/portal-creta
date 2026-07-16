# Logos dos clientes

Coloque aqui o logo de cada cliente, nomeado pelo slug dele:

```
logos/ewz.svg
logos/xyz-capital.png
```

Depois de commitar e rodar `firebase deploy --only hosting`, o arquivo fica
acessível publicamente em:

```
https://synciadesk-hosting.web.app/logos/<arquivo>
```

Use essa URL no campo **"URL do logo"** ao cadastrar o cliente em
`admin.synciadesk.com.br`.

Formatos aceitos: qualquer imagem que o navegador exiba (SVG, PNG, JPG). SVG
é preferível — a logo da Creta (`logo-creta.svg`, na raiz do repo, mantida
por compatibilidade) usa esse formato.
