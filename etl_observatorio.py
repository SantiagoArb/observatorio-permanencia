"""
ETL del Observatorio de Permanencia - Universidad Andina del Norte (caso académico)

Uso:
    python etl_observatorio.py <carpeta_csv_originales> <carpeta_salida>

Lee los cinco CSV originales (sin tocarlos), aplica las reglas de limpieza e
integración y genera:
    - cohorte_2025_1_limpia.csv       tabla limpia de la cohorte (1.200 filas, sin datos personales)
    - fact_permanencia.csv            tabla de hechos del modelo estrella
    - dim_estudiante.csv, dim_programa.csv, dim_periodo.csv, dim_estrato.csv
    - registro_transformaciones.csv   cada regla aplicada y cuántas filas tocó
    - indicadores.csv                 los indicadores oficiales calculados
    - observatorio_looker.xlsx        las mismas tablas en un libro para Google Sheets / Looker Studio

El proceso se puede ejecutar cuantas veces se quiera sobre los CSV originales:
siempre parte de cero y no guarda nada a mano.
"""

import hashlib
import sys
import unicodedata
from pathlib import Path

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Parámetros
# ---------------------------------------------------------------------------
HOMOLOGACION_PROGRAMA = {
    # SIS (variantes)
    "ADMON EMPRESAS": "Administración de Empresas",
    "ING. INFORMATICA": "Ingeniería Informática",
    "PSICOLOGIA": "Psicología",
    "ING INDUSTRIAL": "Ingeniería Industrial",
    "CONTADURIA": "Contaduría Pública",
    "TRABAJO SOCIAL": "Trabajo Social",
    # Bienestar (abreviaturas)
    "Adm.": "Administración de Empresas",
    "Ing. Inf.": "Ingeniería Informática",
    "Psico.": "Psicología",
    "Ing. Ind.": "Ingeniería Industrial",
    "Cont.": "Contaduría Pública",
    "T. Social": "Trabajo Social",
}

# Los datos no traen facultad: se asigna con esta tabla institucional.
PROGRAMAS = [
    # programa_key, programa, sigla, facultad
    ("P01", "Administración de Empresas", "ADM", "Ciencias Económicas y Administrativas"),
    ("P02", "Contaduría Pública", "CON", "Ciencias Económicas y Administrativas"),
    ("P03", "Ingeniería Industrial", "IND", "Ingenierías"),
    ("P04", "Ingeniería Informática", "INF", "Ingenierías"),
    ("P05", "Psicología", "PSI", "Ciencias Sociales y Humanas"),
    ("P06", "Trabajo Social", "TSO", "Ciencias Sociales y Humanas"),
]

ESTRATOS = [
    (1, "Estrato 1", "Bajo"), (2, "Estrato 2", "Bajo"),
    (3, "Estrato 3", "Medio"), (4, "Estrato 4", "Medio"),
    (5, "Estrato 5", "Alto"), (6, "Estrato 6", "Alto"),
]

PERIODOS = [
    (20251, "2025-1", 2025, 1, "Ingreso de la cohorte"),
    (20252, "2025-2", 2025, 2, "Primer seguimiento"),
]

MESES = {"ene": 1, "feb": 2, "mar": 3, "abr": 4, "may": 5, "jun": 6,
         "jul": 7, "ago": 8, "sep": 9, "oct": 10, "nov": 11, "dic": 12}

# Regla actual de alerta temprana (definición oficial)
UMBRAL_NOTA_ACTUAL = 3.0          # promedio de notas parciales semanas 4 y 8, escala 0-5
# Regla nueva propuesta (Reto 5)
UMBRAL_ACCESOS_NUEVA = 8          # suma de accesos semanas 5 a 8

log = []


def registrar(etapa, archivo, problema, regla, filas):
    log.append({"n": len(log) + 1, "etapa": etapa, "archivo": archivo,
                "problema": problema, "regla": regla, "filas_afectadas": int(filas)})


def leer(carpeta, nombre):
    return pd.read_csv(Path(carpeta) / nombre, dtype=str, encoding="utf-8-sig", keep_default_na=False)


def limpiar_documento(serie):
    return serie.str.replace(".", "", regex=False).str.strip()


def clave_nombre(texto):
    """Nombre sin tildes, en mayúsculas y sin espacios extra, para poder cruzar."""
    sin_tildes = "".join(c for c in unicodedata.normalize("NFD", texto) if unicodedata.category(c) != "Mn")
    return " ".join(sin_tildes.upper().split())


def parsear_fecha(texto):
    t = texto.strip()
    if len(t) == 10 and t[4] == "-":                       # 2024-12-31
        return pd.Timestamp(int(t[:4]), int(t[5:7]), int(t[8:]))
    if len(t) == 10 and t[2] == "/":                       # 31/12/2024
        return pd.Timestamp(int(t[6:]), int(t[3:5]), int(t[:2]))
    dia, mes, anio = t.split("-")                          # 31-dic-2024
    return pd.Timestamp(int(anio), MESES[mes.lower()], int(dia))


def a_numero(texto):
    """Convierte '2,6' o '3.5' o '36' a número; vacío -> NaN."""
    if texto is None or str(texto).strip() == "":
        return np.nan
    return float(str(texto).replace(",", "."))


def seudonimo(documento):
    return "EST-" + hashlib.sha256(("andina-2025|" + documento).encode()).hexdigest()[:10].upper()


def main(entrada, salida):
    salida = Path(salida)
    salida.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------ E
    adm = leer(entrada, "admisiones.csv")
    sis = leer(entrada, "sis_matriculas.csv")
    erp = leer(entrada, "erp_pagos_2025_2.csv")
    moo = leer(entrada, "moodle_actividad_2025_1.csv")
    bie = leer(entrada, "bienestar_registros_2025_1.csv")
    registrar("Extracción", "todos", "Archivos con BOM UTF-8 y celdas vacías",
              "Leer como texto con codificación utf-8-sig; nada se modifica en el origen",
              len(adm) + len(sis) + len(erp) + len(moo) + len(bie))

    # ------------------------------------------------------------------ T: SIS
    n = (limpiar_documento(sis.documento) != sis.documento).sum()
    sis["documento"] = limpiar_documento(sis.documento)
    registrar("Limpieza", "sis_matriculas", "Documento con puntos de miles (1.000.902.633)",
              "Quitar puntos y espacios; el documento se trata como texto", n)

    n = (sis.periodo == "2025-II").sum()
    sis["periodo"] = sis.periodo.replace({"2025-II": "2025-2"})
    registrar("Limpieza", "sis_matriculas", "Periodo escrito '2025-II'", "Reemplazar por '2025-2'", n)

    n = sis.programa.isin(HOMOLOGACION_PROGRAMA).sum()
    sis["programa"] = sis.programa.replace(HOMOLOGACION_PROGRAMA)
    registrar("Limpieza", "sis_matriculas", "Programa con nombres no estándar (ADMON EMPRESAS)",
              "Homologar con la tabla oficial de programas", n)

    prom = sis.promedio_periodo.map(a_numero)
    n_coma = sis.promedio_periodo.str.contains(",", regex=False).sum()
    n_escala = (prom > 5).sum()
    sis["promedio_periodo"] = np.where(prom > 5, prom / 10, prom)
    registrar("Limpieza", "sis_matriculas", "Promedio con coma decimal ('2,6')",
              "Cambiar coma por punto y convertir a número", n_coma)
    registrar("Limpieza", "sis_matriculas", "Promedio fuera de escala 0-5 (36 en vez de 3,6)",
              "Si el promedio es mayor que 5, dividir entre 10", n_escala)

    antes = len(sis)
    sis = sis.drop_duplicates(subset=["documento", "periodo"], keep="first")
    registrar("Limpieza", "sis_matriculas", "Filas duplicadas", "Conservar una fila por documento + periodo",
              antes - len(sis))

    n_transf = (sis.tipo_ingreso == "Transferencia").sum()
    cohorte = sis[(sis.periodo == "2025-1") & (sis.tipo_ingreso == "Nuevo")].copy()
    registrar("Integración", "sis_matriculas", "Transferencias entrantes de 2025-2 mezcladas con la cohorte",
              "Cohorte = periodo 2025-1 y tipo de ingreso 'Nuevo' (se excluyen transferencias)", n_transf)
    assert len(cohorte) == 1200, f"La cohorte debería tener 1.200 estudiantes y tiene {len(cohorte)}"

    matriculados_2025_2 = set(sis.loc[sis.periodo == "2025-2", "documento"])
    cohorte["matriculado_2025_2"] = cohorte.documento.isin(matriculados_2025_2).astype(int)
    cohorte["desertor"] = 1 - cohorte.matriculado_2025_2

    # ------------------------------------------------------------------ T: Admisiones
    adm["documento"] = limpiar_documento(adm.documento)
    antes = len(adm)
    adm = adm.drop_duplicates(subset=["documento"], keep="first").copy()
    registrar("Limpieza", "admisiones", "Admitidos duplicados (nombre repetido en MAYÚSCULAS)",
              "Conservar una fila por documento", antes - len(adm))

    formatos = adm.fecha_admision.str.strip()
    n_fmt = (~formatos.str.match(r"^\d{2}/\d{2}/\d{4}$")).sum()
    adm["fecha_admision"] = formatos.map(parsear_fecha)
    registrar("Limpieza", "admisiones", "Fecha en tres formatos (31/12/2024, 2024-12-31, 31-dic-2024)",
              "Convertir cada formato a fecha (AAAA-MM-DD)", n_fmt)
    adm["estrato"] = adm.estrato.astype(int)
    adm["puntaje_saber11"] = adm.puntaje_saber11.astype(int)
    adm["nombre_clave"] = adm.nombre_completo.map(clave_nombre)

    cohorte = cohorte.merge(adm[["documento", "nombre_clave", "estrato", "tipo_colegio", "puntaje_saber11",
                                 "fecha_admision"]], on="documento", how="left", validate="one_to_one")
    assert cohorte.estrato.notna().all()

    # ------------------------------------------------------------------ T: ERP
    erp["documento"] = limpiar_documento(erp.documento)
    n = (erp.estado_pago != erp.estado_pago.str.strip().str.capitalize()).sum()
    erp["estado_pago"] = erp.estado_pago.str.strip().str.capitalize()
    registrar("Limpieza", "erp_pagos_2025_2", "Estado de pago con mayúsculas inconsistentes (PENDIENTE)",
              "Unificar a Pagado / Pendiente / Anulado", n)
    erp["valor_liquidado"] = erp.valor_liquidado.astype(int)
    cohorte = cohorte.merge(erp[["documento", "estado_pago", "valor_liquidado"]], on="documento", how="left",
                            validate="one_to_one")
    cohorte["estado_pago"] = cohorte.estado_pago.fillna("Sin liquidación")
    registrar("Integración", "erp_pagos_2025_2", "El ERP no compartía llave con el SIS (documento con puntos)",
              "Cruzar por documento limpio; sin cruce = 'Sin liquidación'",
              (cohorte.estado_pago == "Sin liquidación").sum())

    # ------------------------------------------------------------------ T: Moodle
    # Moodle exporta 16 filas por estudiante, en el mismo orden del código estudiantil.
    cohorte = cohorte.sort_values("codigo").reset_index(drop=True)
    correos_bloque = moo.correo.iloc[::16].tolist()
    assert len(moo) == 16 * len(cohorte), "Moodle debe traer 16 semanas por estudiante"
    assert correos_bloque == cohorte.correo_institucional.tolist(), "El orden de Moodle no coincide con el código"
    moo["codigo"] = np.repeat(cohorte.codigo.values, 16)
    compartidos = cohorte.correo_institucional.duplicated(keep=False).sum()
    registrar("Integración", "moodle_actividad_2025_1", "Dos estudiantes comparten el mismo correo (32 filas mezcladas)",
              "Asignar cada bloque de 16 semanas al código estudiantil según su posición, no por correo", compartidos)

    moo["semana"] = moo.semana.astype(int)
    moo["accesos"] = moo.accesos.astype(int)
    moo["nota"] = moo.nota_parcial_0_100.map(a_numero) / 20  # escala 0-5
    acc = moo.pivot(index="codigo", columns="semana", values="accesos")
    nota = moo.pivot(index="codigo", columns="semana", values="nota")
    registrar("Limpieza", "moodle_actividad_2025_1", "Nota parcial en escala 0-100",
              "Convertir a escala 0-5 dividiendo entre 20", moo.nota.notna().sum())

    feats = pd.DataFrame(index=acc.index)
    for k in range(1, 9):
        feats[f"accesos_sem_{k}"] = acc[k]
    feats["accesos_sem_1_8"] = acc.loc[:, 1:8].sum(axis=1)
    feats["accesos_sem_5_8"] = acc.loc[:, 5:8].sum(axis=1)
    feats["accesos_sem_7_8"] = acc[7] + acc[8]
    feats["nota_parcial_s4"] = nota[4]
    feats["nota_parcial_s8"] = nota[8]
    feats["nota_parcial_s4_s8"] = (nota[4] + nota[8]) / 2
    cohorte = cohorte.merge(feats, left_on="codigo", right_index=True, how="left", validate="one_to_one")

    cohorte["alerta_actual"] = ((cohorte.nota_parcial_s4_s8 < UMBRAL_NOTA_ACTUAL) |
                                (cohorte.accesos_sem_7_8 == 0)).astype(int)
    cohorte["alerta_nueva"] = (cohorte.accesos_sem_5_8 <= UMBRAL_ACCESOS_NUEVA).astype(int)

    # ------------------------------------------------------------------ T: Bienestar
    bie["programa"] = bie.programa.replace(HOMOLOGACION_PROGRAMA)
    registrar("Limpieza", "bienestar_registros_2025_1", "Programa abreviado (Adm., Psico.)",
              "Homologar con la tabla oficial de programas", len(bie))
    bie["nombre_clave"] = bie.nombre_completo.map(clave_nombre)
    homonimos = cohorte.groupby("nombre_clave").programa.transform("nunique").gt(1).sum()
    llave = cohorte[["documento", "nombre_clave", "programa"]]
    assert not llave.duplicated(["nombre_clave", "programa"]).any(), "Hay homónimos dentro del mismo programa"
    bie = bie.merge(llave, on=["nombre_clave", "programa"], how="left", validate="many_to_one")
    registrar("Integración", "bienestar_registros_2025_1", "Bienestar no registra documento y hay homónimos",
              "Cruzar por nombre normalizado + programa homologado; los homónimos quedan separados por programa",
              homonimos)
    assert bie.documento.notna().all(), "Hay registros de Bienestar sin estudiante"
    conteo = bie.pivot_table(index="documento", columns="tipo_registro", values="fecha", aggfunc="count", fill_value=0)
    cohorte["cancelacion_formal"] = cohorte.documento.map(conteo.get("Cancelación formal", pd.Series(dtype=int))).fillna(0).astype(int)
    cohorte["alertas_docentes"] = cohorte.documento.map(conteo.get("Alerta docente", pd.Series(dtype=int))).fillna(0).astype(int)
    registrar("Carga", "bienestar_registros_2025_1", "Observaciones y atenciones psicológicas son datos sensibles",
              "Se publican solo conteos de cancelaciones y alertas; nombres, observaciones y atenciones no salen de la zona protegida",
              (bie.tipo_registro == "Atención psicológica").sum())

    # ------------------------------------------------------------------ Seudonimización
    cohorte["estudiante_key"] = cohorte.documento.map(seudonimo)
    adm["estudiante_key"] = adm.documento.map(seudonimo)
    registrar("Carga", "todos", "Documento, nombre y correo son datos personales",
              "Reemplazar por una llave seudónima (hash) antes de publicar", len(adm))

    # ------------------------------------------------------------------ Dimensiones
    dim_programa = pd.DataFrame(PROGRAMAS, columns=["programa_key", "programa", "sigla", "facultad"])
    dim_estrato = pd.DataFrame(ESTRATOS, columns=["estrato_key", "estrato", "nivel_estrato"])
    dim_periodo = pd.DataFrame(PERIODOS, columns=["periodo_key", "periodo", "anio", "semestre", "etapa"])
    prog_key = dict(zip(dim_programa.programa, dim_programa.programa_key))

    adm["en_cohorte"] = adm.documento.isin(cohorte.documento).astype(int)
    adm["rango_saber11"] = pd.cut(adm.puntaje_saber11, [0, 249, 299, 349, 500],
                                  labels=["Menos de 250", "250-299", "300-349", "350 o más"]).astype(str)
    dim_estudiante = adm[["estudiante_key", "tipo_colegio", "puntaje_saber11", "rango_saber11"]].copy()
    dim_estudiante["condicion"] = np.where(adm.en_cohorte == 1, "Cohorte 2025-1", "Admitido no matriculado")
    dim_estudiante["fecha_admision"] = adm.fecha_admision.dt.strftime("%Y-%m-%d")

    # ------------------------------------------------------------------ Hechos
    # Granularidad: una fila por estudiante por periodo académico.
    #   2025-1: los 1.450 admitidos únicos (en_cohorte = 1 para los 1.200 que se matricularon)
    #   2025-2: los 1.200 estudiantes de la cohorte (retenido = 1 si se matricularon)
    c = cohorte.set_index("documento")
    h1 = pd.DataFrame({
        "estudiante_key": adm.estudiante_key.values,
        "periodo_key": 20251,
        "programa_key": adm.programa.map(prog_key).values,
        "estrato_key": adm.estrato.values,
        "admitido": 1,
        "en_cohorte": adm.en_cohorte.values,
    })
    doc1 = adm.documento.values
    get = lambda col: c[col].reindex(doc1).values
    h1["matriculado"] = h1.en_cohorte
    h1["retenido"] = 0
    h1["desertor"] = 0
    h1["creditos"] = pd.to_numeric(pd.Series(get("creditos")), errors="coerce").values
    h1["promedio_periodo"] = get("promedio_periodo")
    h1["accesos_sem_1_8"] = get("accesos_sem_1_8")
    h1["accesos_sem_5_8"] = get("accesos_sem_5_8")
    h1["nota_parcial_s4_s8"] = get("nota_parcial_s4_s8")
    h1["alerta_actual"] = get("alerta_actual")
    h1["alerta_nueva"] = get("alerta_nueva")
    h1["cancelacion_formal"] = get("cancelacion_formal")
    h1["alertas_docentes"] = get("alertas_docentes")
    h1["valor_liquidado"] = np.nan
    h1["pago_pendiente"] = np.nan

    alerta_act = pd.Series(get("alerta_actual"), dtype="float")
    alerta_nue = pd.Series(get("alerta_nueva"), dtype="float")
    deserto = pd.Series(get("desertor"), dtype="float")
    h1["alerta_actual_desertor"] = (alerta_act * deserto).values
    h1["alerta_nueva_desertor"] = (alerta_nue * deserto).values

    h2 = pd.DataFrame({
        "estudiante_key": cohorte.estudiante_key.values,
        "periodo_key": 20252,
        "programa_key": cohorte.programa.map(prog_key).values,
        "estrato_key": cohorte.estrato.values,
        "admitido": 0,
        "en_cohorte": 0,
        "matriculado": cohorte.matriculado_2025_2.values,
        "retenido": cohorte.matriculado_2025_2.values,
        "desertor": cohorte.desertor.values,
    })
    sis2 = sis[sis.periodo == "2025-2"].set_index("documento")
    h2["creditos"] = pd.to_numeric(sis2.creditos.reindex(cohorte.documento), errors="coerce").values
    h2["valor_liquidado"] = cohorte.valor_liquidado.values
    h2["pago_pendiente"] = (cohorte.estado_pago == "Pendiente").astype(int).values
    fact = pd.concat([h1, h2], ignore_index=True)
    fact["con_promedio"] = fact.promedio_periodo.notna().astype(int)
    fact = fact[["estudiante_key", "periodo_key", "programa_key", "estrato_key",
                 "admitido", "en_cohorte", "matriculado", "retenido", "desertor",
                 "creditos", "promedio_periodo", "con_promedio", "accesos_sem_1_8", "accesos_sem_5_8", "nota_parcial_s4_s8",
                 "alerta_actual", "alerta_actual_desertor", "alerta_nueva", "alerta_nueva_desertor",
                 "cancelacion_formal", "alertas_docentes", "valor_liquidado", "pago_pendiente"]]
    assert not fact.duplicated(["estudiante_key", "periodo_key"]).any()

    # ------------------------------------------------------------------ Indicadores
    tot_coh = fact.en_cohorte.sum()
    des = fact.desertor.sum()
    ind = [
        ("Absorción", fact.en_cohorte.sum() / fact.admitido.sum(), "Estudiantes de la cohorte / admitidos únicos"),
        ("Retención de primer periodo", fact.retenido.sum() / tot_coh, "Cohorte matriculada en 2025-2 / cohorte"),
        ("Deserción", des / tot_coh, "100 % - retención"),
        ("Promedio de la cohorte", np.nanmean(fact.loc[fact.en_cohorte == 1, "promedio_periodo"].astype(float)),
         "Promedio 2025-1 (0-5) de los estudiantes con nota registrada"),
        ("Alertados (regla actual)", fact.alerta_actual.sum(), "Nota parcial s4-s8 < 3,0 o 0 accesos en s7+s8"),
        ("Precisión de la alerta actual", fact.alerta_actual_desertor.sum() / fact.alerta_actual.sum(),
         "Alertados que desertaron / alertados"),
        ("Cobertura de la alerta actual", fact.alerta_actual_desertor.sum() / des,
         "Alertados que desertaron / desertores"),
        ("Alertados (regla nueva)", fact.alerta_nueva.sum(), "Accesos semanas 5 a 8 <= 8"),
        ("Precisión de la alerta nueva", fact.alerta_nueva_desertor.sum() / fact.alerta_nueva.sum(),
         "Alertados que desertaron / alertados"),
        ("Cobertura de la alerta nueva", fact.alerta_nueva_desertor.sum() / des,
         "Alertados que desertaron / desertores"),
    ]
    indicadores = pd.DataFrame(ind, columns=["indicador", "valor", "definicion"])

    por_programa = (cohorte.groupby("programa")
                    .agg(cohorte=("desertor", "size"), desertores=("desertor", "sum"))
                    .assign(desercion=lambda x: x.desertores / x.cohorte).reset_index())

    # ------------------------------------------------------------------ Tabla limpia de la cohorte (pública)
    cohorte = cohorte.merge(dim_programa[["programa", "facultad"]], on="programa", how="left")
    publica = cohorte[["estudiante_key", "programa", "facultad", "estrato", "tipo_colegio", "puntaje_saber11",
                       "creditos", "promedio_periodo",
                       *[f"accesos_sem_{k}" for k in range(1, 9)], "accesos_sem_5_8", "accesos_sem_7_8",
                       "nota_parcial_s4", "nota_parcial_s8", "nota_parcial_s4_s8",
                       "alerta_actual", "alerta_nueva", "cancelacion_formal", "alertas_docentes",
                       "estado_pago", "matriculado_2025_2", "desertor"]].copy()

    # ------------------------------------------------------------------ Carga
    def guardar(df, nombre):
        df.to_csv(salida / nombre, index=False, encoding="utf-8-sig")

    guardar(publica, "cohorte_2025_1_limpia.csv")
    guardar(fact, "fact_permanencia.csv")
    guardar(dim_estudiante, "dim_estudiante.csv")
    guardar(dim_programa, "dim_programa.csv")
    guardar(dim_periodo, "dim_periodo.csv")
    guardar(dim_estrato, "dim_estrato.csv")
    guardar(pd.DataFrame(log), "registro_transformaciones.csv")
    guardar(indicadores, "indicadores.csv")
    guardar(por_programa, "desercion_por_programa.csv")

    with pd.ExcelWriter(salida / "observatorio_looker.xlsx") as xl:
        fact.to_excel(xl, "fact_permanencia", index=False)
        dim_estudiante.to_excel(xl, "dim_estudiante", index=False)
        dim_programa.to_excel(xl, "dim_programa", index=False)
        dim_periodo.to_excel(xl, "dim_periodo", index=False)
        dim_estrato.to_excel(xl, "dim_estrato", index=False)

    print(f"Cohorte: {len(publica)} estudiantes")
    print(indicadores.to_string(index=False))
    print(por_programa.to_string(index=False))
    return indicadores


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(1)
    main(sys.argv[1], sys.argv[2])
