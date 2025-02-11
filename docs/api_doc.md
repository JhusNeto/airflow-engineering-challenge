# API de Teste - Sistema de Vendas

Esta é uma API REST que simula um sistema de vendas com produtos, carrinhos, clientes e logística.

## Endpoints Disponíveis

### Autenticação

Para acessar os endpoints da API, você precisa obter um token de acesso:

**Login para obter token:**
```
POST /token

Body (form-data):
username: admin
password: admin

Resposta:
{
    "access_token": "seu_token_jwt",
    "refresh_token": "seu_refresh_token",
    "token_type": "bearer"
}
```

**Renovar token expirado:**
```
POST /refresh-token
Header: Authorization: Bearer seu_refresh_token

Resposta:
{
    "access_token": "novo_token_jwt",
    "token_type": "bearer"
}
```

### Endpoints da API

Todos os endpoints requerem autenticação via Bearer token no header:
```
Authorization: Bearer seu_token_jwt
```

#### Produtos
```
GET /api/v1/products?skip=0&limit=50

Resposta:
[
    {
        "id": 1,
        "name": "Produto 1",
        "category": "Categoria",
        "price": 99.99,
        "created_at": "2023-01-01T00:00:00"
    }
]
```

#### Carrinhos/Vendas
```
GET /api/v1/carts?skip=0&limit=50

Resposta:
[
    {
        "id": 1,
        "customer_id": 1,
        "logistic_id": 1,
        "sale_date": "2023-01-01T00:00:00",
        "status": "completed",
        "total_amount": 99.99,
        "items": [...],
        "shipping_info": {...},
        "payment_info": {...}
    }
]
```

#### Clientes
```
GET /api/v1/customer?skip=0&limit=50

Resposta:
[
    {
        "id": 1,
        "full_name": "Nome Cliente",
        "email": "email@exemplo.com",
        "phone": "999999999",
        "address": "Endereço",
        "city": "Cidade"
    }
]
```

#### Logística
```
GET /api/v1/logistict?skip=0&limit=50

Resposta:
[
    {
        "id": 1,
        "company_name": "Empresa",
        "service_type": "Tipo Serviço",
        "contact_phone": "999999999",
        "origin_warehouse": "Local Armazém"
    }
]
```

## Paginação

Todos os endpoints suportam paginação através dos parâmetros:
- `skip`: número de registros para pular (default: 0)
- `limit`: número de registros por página (default: 50, máximo: 50)

## Observações Importantes

1. A API pode apresentar instabilidades e retornar erros 500 de forma aleatória
2. Implemente tratamento adequado de erros e retentativas
3. O token de acesso expira em 30 minutos
4. Use o refresh_token para obter um novo token de acesso quando necessário
5. Todos os timestamps estão em UTC 