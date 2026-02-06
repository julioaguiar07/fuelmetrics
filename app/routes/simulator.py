from fastapi import APIRouter, Query, HTTPException
import logging
import math
from datetime import datetime
from app.models.schemas import SimulatorRequest, SimulatorResponse, FuelType
from app.services.anp_downloader import ANPDownloader
from app.services.data_processor import DataProcessor

router = APIRouter()
logger = logging.getLogger(__name__)

def get_processor():
    """Obter processador de dados"""
    from app.routes.today import get_processor as get_global_processor
    return get_global_processor()

@router.get("/calculate", response_model=SimulatorResponse)
async def calculate_trip(
    tank_capacity: float = Query(..., gt=0, description="Capacidade do tanque em litros"),
    current_level: float = Query(..., ge=0, le=100, description="Nível atual do tanque em %"),
    consumption: float = Query(..., gt=0, description="Consumo médio em km/L"),
    distance: float = Query(..., gt=0, description="Distância da viagem em km"),
    fuel_type: FuelType = Query(FuelType.GASOLINA, description="Tipo de combustível"),
    city: str = Query(None, description="Cidade para obter preço (opcional)")
):
    """Calcula viabilidade de uma viagem baseada no combustível disponível"""
    try:
        # Validar inputs
        if current_level < 0 or current_level > 100:
            raise HTTPException(
                status_code=400,
                detail="Nível atual deve estar entre 0 e 100%"
            )
        
        # Calcular litros atuais
        current_liters = tank_capacity * (current_level / 100)
        
        # Calcular autonomia
        current_autonomy = current_liters * consumption
        
        # Calcular combustível necessário
        fuel_needed = distance / consumption
        required_autonomy = distance
        
        # Calcular combustível restante
        remaining_liters = current_liters - fuel_needed
        remaining_percent = (remaining_liters / tank_capacity) * 100
        
        # Calcular margem de segurança (20% da autonomia)
        safety_margin = current_autonomy * 0.2
        
        # Obter preço do combustível se cidade fornecida
        estimated_cost = None
        if city:
            try:
                processor = get_processor()
                city_data = processor.df[
                    (processor.df['municipio'] == city.upper()) & 
                    (processor.df['produto_consolidado'] == fuel_type.value.upper())
                ]
                
                if not city_data.empty:
                    avg_price = float(city_data['preco_medio_revenda'].mean())
                    estimated_cost = fuel_needed * avg_price
            except:
                pass  # Se falhar, continuar sem preço estimado
        
        # Determinar status
        if remaining_liters < 0:
            # Combustível insuficiente
            shortage = abs(remaining_liters)
            shortage_km = shortage * consumption
            
            status = "danger"
            message = (
                f"Combustível insuficiente. Faltam {shortage:.1f} litros "
                f"({shortage_km:.0f} km) para completar a viagem."
            )
            
        elif remaining_percent < 10:
            # Chega na reserva
            status = "warning"
            message = (
                f"Você chegará com apenas {remaining_percent:.1f}% no tanque "
                f"({remaining_liters:.1f} litros). Recomenda-se abastecer antes."
            )
            
        elif remaining_percent < 20:
            # Baixo mas seguro
            status = "warning"
            message = (
                f"Você chegará com {remaining_percent:.1f}% no tanque "
                f"({remaining_liters:.1f} litros). Considere abastecer durante a viagem."
            )
            
        else:
            # Seguro
            status = "safe"
            message = (
                f"Viagem segura! Você chegará com {remaining_percent:.1f}% no tanque "
                f"({remaining_liters:.1f} litros restantes)."
            )
        
        return SimulatorResponse(
            current_autonomy=round(current_autonomy, 1),
            required_autonomy=round(required_autonomy, 1),
            remaining_liters=round(max(0, remaining_liters), 1),
            remaining_percent=round(max(0, remaining_percent), 1),
            fuel_needed=round(fuel_needed, 1),
            status=status,
            message=message,
            estimated_cost=round(estimated_cost, 2) if estimated_cost else None,
            safety_margin=round(safety_margin, 1)
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro em /calculate: {e}")
        raise HTTPException(status_code=500, detail="Erro interno do servidor")

@router.post("/calculate", response_model=SimulatorResponse)
async def calculate_trip_post(request: SimulatorRequest):
    """Versão POST do simulador"""
    return await calculate_trip(
        tank_capacity=request.tank_capacity,
        current_level=request.current_level,
        consumption=request.consumption,
        distance=request.distance,
        fuel_type=request.fuel_type,
        city=None
    )

@router.get("/optimize")
async def optimize_refuel(
    current_level: float = Query(..., ge=0, le=100, description="Nível atual do tanque em %"),
    tank_capacity: float = Query(50, gt=0, description="Capacidade do tanque em litros"),
    consumption: float = Query(12, gt=0, description="Consumo médio em km/L"),
    trip_distance: float = Query(300, gt=0, description="Distância total da viagem em km"),
    fuel_price: float = Query(5.0, gt=0, description="Preço do combustível em R$/L"),
    safe_reserve: float = Query(20, ge=0, le=50, description="Reserva segura em %")
):
    """Otimiza o ponto de reabastecimento na viagem"""
    try:
        # Calcular litros atuais
        current_liters = tank_capacity * (current_level / 100)
        current_autonomy = current_liters * consumption
        
        # Calcular autonomia necessária
        required_autonomy = trip_distance
        
        # Verificar se precisa abastecer
        if current_autonomy >= trip_distance + (tank_capacity * (safe_reserve/100) * consumption):
            # Não precisa abastecer
            return {
                "need_refuel": False,
                "reason": f"Autonomia suficiente ({current_autonomy:.0f} km) para viagem de {trip_distance} km",
                "remaining_after_trip": round((current_autonomy - trip_distance) / consumption, 1),
                "remaining_percent": round(((current_autonomy - trip_distance) / consumption / tank_capacity) * 100, 1)
            }
        
        # Calcular ponto ideal de abastecimento
        # Queremos abastecer quando atingir a reserva segura
        km_to_reserve = (tank_capacity * (safe_reserve/100)) * consumption
        km_before_refuel = current_autonomy - km_to_reserve
        
        if km_before_refuel < 0:
            # Precisa abastecer imediatamente
            refuel_point = 0
            liters_needed = (trip_distance / consumption) - current_liters
        else:
            # Pode viajar um pouco antes de abastecer
            refuel_point = km_before_refuel
            remaining_after_refuel = trip_distance - refuel_point
            liters_needed = remaining_after_refuel / consumption
        
        # Calcular custo
        refuel_cost = liters_needed * fuel_price
        
        # Sugerir quantidade (arredondar para múltiplos de 5 litros)
        suggested_liters = math.ceil(liters_needed / 5) * 5
        suggested_cost = suggested_liters * fuel_price
        
        return {
            "need_refuel": True,
            "refuel_point_km": round(max(0, refuel_point), 1),
            "refuel_point_percent": round((refuel_point / trip_distance) * 100, 1),
            "liters_needed": round(max(0, liters_needed), 1),
            "estimated_cost": round(refuel_cost, 2),
            "suggestion": {
                "liters": suggested_liters,
                "cost": round(suggested_cost, 2),
                "reason": f"Abastecer {suggested_liters}L ({suggested_liters/fuel_price:.2f}L de segurança)"
            },
            "warnings": _generate_refuel_warnings(current_level, safe_reserve)
        }
        
    except Exception as e:
        logger.error(f"Erro em /optimize: {e}")
        raise HTTPException(status_code=500, detail="Erro interno do servidor")

@router.get("/multi-stop")
async def multi_stop_simulation(
    stops: str = Query(..., description="Lista de distâncias entre paradas (km) separadas por vírgula"),
    tank_capacity: float = Query(50, gt=0, description="Capacidade do tanque em litros"),
    initial_level: float = Query(100, ge=0, le=100, description="Nível inicial do tanque em %"),
    consumption: float = Query(12, gt=0, description="Consumo médio em km/L"),
    min_reserve: float = Query(15, ge=5, le=30, description="Reserva mínima em %")
):
    """Simula viagem com múltiplas paradas"""
    try:
        # Parse das distâncias
        distances = [float(d.strip()) for d in stops.split(',')]
        
        if not distances:
            raise HTTPException(status_code=400, detail="Forneça pelo menos uma distância")
        
        # Inicializar variáveis
        current_liters = tank_capacity * (initial_level / 100)
        total_distance = sum(distances)
        route = []
        refuel_points = []
        
        # Simular cada trecho
        for i, distance in enumerate(distances, 1):
            # Calcular combustível necessário para este trecho
            fuel_needed = distance / consumption
            
            # Verificar se tem combustível suficiente
            if current_liters >= fuel_needed + (tank_capacity * (min_reserve/100)):
                # Tem combustível suficiente
                current_liters -= fuel_needed
                status = "ok"
                refuel_need = None
            else:
                # Precisa abastecer antes deste trecho
                liters_to_refuel = tank_capacity - current_liters
                current_liters = tank_capacity  # Enche o tanque
                current_liters -= fuel_needed  # Gasta no trecho
                
                refuel_points.append({
                    "stop": i - 1 if i > 1 else "início",
                    "liters": round(liters_to_refuel, 1),
                    "reason": f"Reserva baixa antes do trecho {i}"
                })
                status = "refueled"
                refuel_need = round(liters_to_refuel, 1)
            
            # Registrar trecho
            route.append({
                "segment": i,
                "distance": distance,
                "fuel_used": round(fuel_needed, 1),
                "remaining_liters": round(current_liters, 1),
                "remaining_percent": round((current_liters / tank_capacity) * 100, 1),
                "status": status,
                "refuel_liters": refuel_need
            })
        
        # Calcular estatísticas finais
        total_fuel_used = total_distance / consumption
        efficiency = total_distance / total_fuel_used
        
        return {
            "total_distance": round(total_distance, 1),
            "total_fuel_used": round(total_fuel_used, 1),
            "average_efficiency": round(efficiency, 1),
            "final_liters": round(current_liters, 1),
            "final_percent": round((current_liters / tank_capacity) * 100, 1),
            "route": route,
            "refuel_points": refuel_points,
            "recommendations": _generate_multi_stop_recommendations(route, refuel_points)
        }
        
    except ValueError:
        raise HTTPException(status_code=400, detail="Distâncias devem ser números")
    except Exception as e:
        logger.error(f"Erro em /multi-stop: {e}")
        raise HTTPException(status_code=500, detail="Erro interno do servidor")

def _generate_refuel_warnings(current_level, safe_reserve):
    """Gera avisos sobre reabastecimento"""
    warnings = []
    
    if current_level < 20:
        warnings.append({
            "level": "high",
            "message": "Nível muito baixo! Abasteça imediatamente.",
            "action": "refuel_now"
        })
    elif current_level < safe_reserve:
        warnings.append({
            "level": "medium",
            "message": f"Nível abaixo da reserva segura ({safe_reserve}%).",
            "action": "plan_refuel"
        })
    
    return warnings

def _generate_multi_stop_recommendations(route, refuel_points):
    """Gera recomendações para viagem com múltiplas paradas"""
    recommendations = []
    
    # Analisar pontos de reabastecimento
    if len(refuel_points) > 3:
        recommendations.append({
            "type": "efficiency",
            "message": "Muitos pontos de reabastecimento. Considere um veículo com maior autonomia.",
            "priority": "medium"
        })
    
    # Verificar se há trechos longos sem reabastecimento
    long_segments = [seg for seg in route if seg['distance'] > 300]
    if long_segments:
        recommendations.append({
            "type": "safety",
            "message": "Trechos muito longos detectados. Verifique postos no caminho.",
            "priority": "high"
        })
    
    # Sugerir pontos de reabastecimento estratégicos
    if not refuel_points and any(seg['remaining_percent'] < 20 for seg in route):
        recommendations.append({
            "type": "planning",
            "message": "Considere reabastecer antes que o nível fique muito baixo.",
            "priority": "medium"
        })
    
    return recommendations
