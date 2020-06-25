import ezdxf
import pandas as pd

if __name__ == '__main__':

    # # get all lines to draw the map
    # for e in msp.query("LINE"):
    #     print(e.dxf.layer)
    #     print(e.dxf.start)
    #     print(e.dxf.end)
    #     print("\n")
    #
    # # get all text fields for the locations of the rooms
    # for e in msp.query('MTEXT'):
    #     print(e.text)
    #     print(e.dxf.insert)
    #     print("\n")

    # TODO: Possibly normalize floor coordinates to allow positioning relatively

    label_positions = []

    # TODO: For each floor plan, DO
    doc = ezdxf.readfile("./data/raw/cad_data/cad_033O.dxf")
    building_id = "033"
    floor_id = "O"
    msp = doc.modelspace()

    # Extract positions of all text labels (Contains room names)
    for entity in msp.query("MTEXT"):
        # Qualify text labels with floor name if possible (else file name)
        label_positions.append([building_id, floor_id, entity.text, entity.dxf.insert[0], entity.dxf.insert[1]])

    label_positions_df = pd.DataFrame(label_positions, columns=["Building ID", "Floor ID", "Room ID", "X-coordinate", "Y-coordinate"])
    label_positions_df.to_csv(f"./data/processed/cad_maps/building_{building_id}_floor_{floor_id}_labels.csv")

    layer_colors = dict()
    # get layer colors
    for layer in doc.layers:
        #print(layer.dxf.name)
        #print(layer.get_color())
        layer_colors[layer.dxf.name] = layer.get_color()

    line_coordinates = []
    for entity in msp.query("LINE"):
        # Extract all lines (to create holoviz graph overlay)
        line_coordinates.append([entity.dxf.layer, layer_colors[entity.dxf.layer], entity.dxf.handle, entity.dxf.start[0], entity.dxf.start[1], entity.dxf.end[0], entity.dxf.end[1]])

    line_coordinates_df = pd.DataFrame(line_coordinates, columns=["Line Layer", "Color ID", "Line ID", "Start X-coordinate", "Start Y-coordinate", "End X-coordinate", "End Y-coordinate"])
    line_coordinates_df.to_csv(f"./data/processed/cad_maps/building_{building_id}_floor_{floor_id}_lines.csv")

    # # TODO: Filter lines by color (black is general floor plan, gray is more detailed (doors, toilets, etc.)
    # # TODO: Store lines and some line details with pandas to allow drawing


