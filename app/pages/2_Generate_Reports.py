import os
import sys
sys.path.append(os.environ['PROJECT_HOME'])

import streamlit as st
import pandas as pd
import numpy as np
import logging
import traceback
from tqdm import tqdm
import time
import io
import base64
from datetime import datetime
from st_aggrid import AgGrid, GridOptionsBuilder
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import matplotlib.pyplot as plt
import seaborn as sns
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter, landscape
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch

from commons.database import pgDb
from commons.streamlitDfs import stContainersDf
from commons.encryptionKey import encryptionKey
from commons.auth import userAuth

logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)
file_handler = logging.FileHandler(f'{os.environ["PROJECT_HOME"]}/logs/app.log')
file_handler.setLevel(logging.ERROR)
formatter = logging.Formatter('%(asctime)s : %(name)s : %(levelname)s :: %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

st.set_page_config(
    page_title="Generate Volume Sessions Reports",
    page_icon="🏠",
    initial_sidebar_state="expanded",
    layout='wide'    
)
st.title("Generate Volume Sessions Reports")


def create_selectors(df, height=200, is_volume_table=False):
    # Configure grid options
    gb = GridOptionsBuilder.from_dataframe(df)
    
    # Configure columns with specific filter types
    for column_name in list(df.columns):
        # For volume table, adjust column width and make sure all columns are visible
        if is_volume_table:
            gb.configure_column(column_name, 
                filter=True,
                filterParams={
                    "filterOptions": ['contains', 'notContains', 'equals', 'notEqual'],
                    "suppressAndOrCondition": True,
                    "maxNumConditions": 5,
                    "buttons": ['clear']
                },
                type=["numericColumn", "numberColumnFilter"],
                width=120,  # Set fixed width for better visibility
                resizable=True  # Allow user to resize if needed
            )
        else:
            gb.configure_column(column_name, 
                filter=True,
                filterParams={
                    "filterOptions": ['contains', 'notContains', 'equals', 'notEqual'],
                    "suppressAndOrCondition": True,
                    "maxNumConditions": 5,
                    "buttons": ['clear']
                },
                type=["numericColumn", "numberColumnFilter"]
            )
    
    # Enable sorting for all columns
    gb.configure_default_column(sorteable=True)
    
    # Configure grid default properties
    if is_volume_table:
        # For volume table, use autoSizeColumns to fit content
        gb.configure_grid_options(domLayout='normal', autoSizeColumns=True)
    else:
        gb.configure_grid_options(domLayout='normal')
    
    # Build grid options
    grid_options = gb.build()

    # Create the AgGrid
    grid_response = AgGrid(
        df, 
        gridOptions=grid_options,
        height=height,
        width='100%',
        data_return_mode='FILTERED',
        update_mode='MODEL_CHANGED',
        fit_columns_on_grid_load=True,
        allow_unsafe_jscode=True,
        theme='alpine'
    )    
    
    # Return all filtered rows
    filtered_df = grid_response['data']
    items_list = filtered_df[df.columns[0]].tolist()

    return items_list

def generate_sessions_pdf(sessions_df, session_users_list, server_list, volume_list, protocol_list, start_date, end_date):
    """
    Generate a PDF report with visualizations of the sessions data
    """
    # Create a file-like buffer to receive PDF data
    buffer = io.BytesIO()
    
    # Create the PDF object using ReportLab
    doc = SimpleDocTemplate(buffer, pagesize=landscape(letter))
    elements = []
    
    # Add styles
    styles = getSampleStyleSheet()
    title_style = styles['Heading1']
    subtitle_style = styles['Heading2']
    normal_style = styles['Normal']
    
    # Add custom style for headers
    header_style = ParagraphStyle(
        'HeaderStyle',
        parent=styles['Heading3'],
        textColor=colors.darkblue,
        spaceAfter=12
    )
    
    # Add title and date
    elements.append(Paragraph(f"Storage Sessions Report", title_style))
    elements.append(Paragraph(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", normal_style))
    elements.append(Spacer(1, 0.25*inch))
    
    # Add report parameters
    elements.append(Paragraph("Report Parameters:", header_style))
    date_range = f"From {start_date} to {end_date}" if start_date and end_date else "All available dates"
    elements.append(Paragraph(f"Date Range: {date_range}", normal_style))
    elements.append(Paragraph(f"Storage Systems: {', '.join(session_users_list[:5])}{'...' if len(session_users_list) > 5 else ''}", normal_style))
    elements.append(Paragraph(f"Protocols: {', '.join(protocol_list)}", normal_style))
    elements.append(Spacer(1, 0.25*inch))
    
    # Create summary statistics
    elements.append(Paragraph("Summary Statistics:", header_style))
    
    # Create a summary table
    summary_data = [
        ["Total Sessions", len(sessions_df)],
        ["Unique Servers", sessions_df['ServerIP'].nunique()],
        ["Unique Volumes", sessions_df['Volume'].nunique()],
        ["Date Range", f"{sessions_df['Timestamp'].min()} to {sessions_df['Timestamp'].max()}"]
    ]
    
    summary_table = Table(summary_data, colWidths=[2*inch, 4*inch])
    summary_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, -1), colors.lightgrey),
        ('TEXTCOLOR', (0, 0), (0, -1), colors.darkblue),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('GRID', (0, 0), (-1, -1), 1, colors.black)
    ]))
    elements.append(summary_table)
    elements.append(Spacer(1, 0.25*inch))
    
    # Create visualizations
    elements.append(Paragraph("Data Visualizations:", header_style))
    
    # 1. Protocol Distribution Pie Chart
    protocol_counts = sessions_df['Protocol'].value_counts()
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.pie(protocol_counts, labels=protocol_counts.index, autopct='%1.1f%%', startangle=90, colors=['#66b3ff', '#99ff99'])
    ax.axis('equal')
    plt.title('Sessions by Protocol')
    
    img_buffer = io.BytesIO()
    plt.savefig(img_buffer, format='png', dpi=300, bbox_inches='tight')
    img_buffer.seek(0)
    protocol_img = Image(img_buffer, width=4*inch, height=3*inch)
    elements.append(protocol_img)
    plt.close()
    
    # 2. Sessions by Server - Horizontal Bar Chart
    server_counts = sessions_df['ServerIP'].value_counts().head(10)
    fig, ax = plt.subplots(figsize=(8, 4))
    sns.barplot(x=server_counts.values, y=server_counts.index, palette='viridis')
    plt.title('Top 10 Servers by Session Count')
    plt.xlabel('Number of Sessions')
    plt.tight_layout()
    
    img_buffer = io.BytesIO()
    plt.savefig(img_buffer, format='png', dpi=300, bbox_inches='tight')
    img_buffer.seek(0)
    server_img = Image(img_buffer, width=6*inch, height=3*inch)
    elements.append(server_img)
    plt.close()
    
    # 3. Sessions by Volume - Horizontal Bar Chart
    volume_counts = sessions_df['Volume'].value_counts().head(10)
    fig, ax = plt.subplots(figsize=(8, 4))
    sns.barplot(x=volume_counts.values, y=volume_counts.index, palette='magma')
    plt.title('Top 10 Volumes by Session Count')
    plt.xlabel('Number of Sessions')
    plt.tight_layout()
    
    img_buffer = io.BytesIO()
    plt.savefig(img_buffer, format='png', dpi=300, bbox_inches='tight')
    img_buffer.seek(0)
    volume_img = Image(img_buffer, width=6*inch, height=3*inch)
    elements.append(volume_img)
    plt.close()
    
    # 4. Time Series of Sessions
    if 'Timestamp' in sessions_df.columns:
        # Convert timestamp to datetime if needed
        if not pd.api.types.is_datetime64_any_dtype(sessions_df['Timestamp']):
            sessions_df['Timestamp'] = pd.to_datetime(sessions_df['Timestamp'])
        
        # Group by date and protocol
        sessions_df['Date'] = sessions_df['Timestamp'].dt.date
        time_series = sessions_df.groupby(['Date', 'Protocol']).size().reset_index(name='Count')
        
        # Pivot for plotting
        time_pivot = time_series.pivot(index='Date', columns='Protocol', values='Count').fillna(0)
        
        # Plot time series
        fig, ax = plt.subplots(figsize=(10, 4))
        time_pivot.plot(ax=ax, marker='o')
        plt.title('Sessions Over Time by Protocol')
        plt.xlabel('Date')
        plt.ylabel('Number of Sessions')
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        
        img_buffer = io.BytesIO()
        plt.savefig(img_buffer, format='png', dpi=300, bbox_inches='tight')
        img_buffer.seek(0)
        time_img = Image(img_buffer, width=7*inch, height=3*inch)
        elements.append(time_img)
        plt.close()
    
    # Add detailed data tables
    elements.append(Paragraph("Top Servers by Session Count:", header_style))
    server_summary = sessions_df.groupby('ServerIP').size().reset_index(name='Sessions')
    server_summary = server_summary.sort_values('Sessions', ascending=False).head(10)
    
    server_table_data = [["Server IP", "Session Count"]]
    for _, row in server_summary.iterrows():
        server_table_data.append([row['ServerIP'], str(row['Sessions'])])
    
    server_table = Table(server_table_data, colWidths=[3*inch, 1*inch])
    server_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.darkblue),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.beige, colors.white])
    ]))
    elements.append(server_table)
    elements.append(Spacer(1, 0.25*inch))
    
    # Build the PDF
    doc.build(elements)
    
    # Get the value of the BytesIO buffer
    pdf_data = buffer.getvalue()
    buffer.close()
    
    return pdf_data

def create_visual_report(sessions_df):
    """
    Create a visual report for the sessions data using Plotly
    """
    if sessions_df is None or len(sessions_df) == 0:
        st.warning("No data available to generate report.")
        return
    
    # Convert timestamp to datetime if needed
    if 'Timestamp' in sessions_df.columns and not pd.api.types.is_datetime64_any_dtype(sessions_df['Timestamp']):
        sessions_df['Timestamp'] = pd.to_datetime(sessions_df['Timestamp'])
    
    # Create tabs for different visualizations
    tab1, tab2, tab3, tab4 = st.tabs(["Protocol Distribution", "Server Analysis", "Volume Analysis", "Time Analysis"])
    
    with tab1:
        st.subheader("Protocol Distribution")
        col1, col2 = st.columns(2)
        
        with col1:
            # Protocol distribution pie chart
            protocol_counts = sessions_df['Protocol'].value_counts().reset_index()
            protocol_counts.columns = ['Protocol', 'Count']
            
            fig = px.pie(
                protocol_counts, 
                values='Count', 
                names='Protocol',
                title='Sessions by Protocol',
                color_discrete_sequence=px.colors.qualitative.Pastel
            )
            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Protocol distribution by storage system
            if 'Storage' in sessions_df.columns:
                protocol_storage = sessions_df.groupby(['Storage', 'Protocol']).size().reset_index(name='Count')
                
                fig = px.bar(
                    protocol_storage,
                    x='Storage',
                    y='Count',
                    color='Protocol',
                    title='Protocol Distribution by Storage System',
                    barmode='group'
                )
                st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        st.subheader("Server Analysis")
        
        # Top servers by session count
        server_counts = sessions_df['ServerIP'].value_counts().reset_index().head(10)
        server_counts.columns = ['ServerIP', 'Count']
        
        fig = px.bar(
            server_counts,
            y='ServerIP',
            x='Count',
            title='Top 10 Servers by Session Count',
            orientation='h',
            color='Count',
            color_continuous_scale='Viridis'
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Server-Volume heatmap
        if len(sessions_df) > 0:
            # Get top 10 servers and volumes for the heatmap
            top_servers = sessions_df['ServerIP'].value_counts().nlargest(10).index.tolist()
            top_volumes = sessions_df['Volume'].value_counts().nlargest(10).index.tolist()
            
            # Filter data for top servers and volumes
            filtered_df = sessions_df[
                sessions_df['ServerIP'].isin(top_servers) & 
                sessions_df['Volume'].isin(top_volumes)
            ]
            
            # Create a pivot table for the heatmap
            heatmap_data = filtered_df.groupby(['ServerIP', 'Volume']).size().reset_index(name='Count')
            heatmap_pivot = heatmap_data.pivot(index='ServerIP', columns='Volume', values='Count').fillna(0)
            
            # Create heatmap
            fig = px.imshow(
                heatmap_pivot,
                labels=dict(x="Volume", y="Server IP", color="Session Count"),
                title="Server-Volume Session Heatmap",
                color_continuous_scale='Viridis'
            )
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)
    
    with tab3:
        st.subheader("Volume Analysis")
        
        # Top volumes by session count
        volume_counts = sessions_df['Volume'].value_counts().reset_index().head(10)
        volume_counts.columns = ['Volume', 'Count']
        
        fig = px.bar(
            volume_counts,
            y='Volume',
            x='Count',
            title='Top 10 Volumes by Session Count',
            orientation='h',
            color='Count',
            color_continuous_scale='Magma'
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Volume usage by protocol
        if 'Protocol' in sessions_df.columns:
            volume_protocol = sessions_df.groupby(['Volume', 'Protocol']).size().reset_index(name='Count')
            volume_protocol = volume_protocol[volume_protocol['Volume'].isin(volume_counts['Volume'].tolist())]
            
            fig = px.bar(
                volume_protocol,
                x='Volume',
                y='Count',
                color='Protocol',
                title='Protocol Usage by Top Volumes',
                barmode='stack'
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with tab4:
        st.subheader("Time Analysis")
        
        if 'Timestamp' in sessions_df.columns:
            # Add date column for grouping
            sessions_df['Date'] = sessions_df['Timestamp'].dt.date
            
            # Sessions over time by protocol
            time_series = sessions_df.groupby(['Date', 'Protocol']).size().reset_index(name='Count')
            
            fig = px.line(
                time_series,
                x='Date',
                y='Count',
                color='Protocol',
                title='Sessions Over Time by Protocol',
                markers=True
            )
            fig.update_layout(xaxis_title='Date', yaxis_title='Number of Sessions')
            st.plotly_chart(fig, use_container_width=True)
            
            # Heatmap of sessions by hour and day of week
            if len(sessions_df) > 0:
                sessions_df['Hour'] = sessions_df['Timestamp'].dt.hour
                sessions_df['DayOfWeek'] = sessions_df['Timestamp'].dt.day_name()
                
                # Order days of week correctly
                day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                
                # Create pivot table for heatmap
                hour_day = sessions_df.groupby(['DayOfWeek', 'Hour']).size().reset_index(name='Count')
                hour_day_pivot = hour_day.pivot(index='DayOfWeek', columns='Hour', values='Count').fillna(0)
                
                # Ensure all 24 hours are represented in columns
                all_hours = list(range(24))
                hour_day_pivot = hour_day_pivot.reindex(columns=all_hours, fill_value=0)
                
                # Reorder index based on day_order
                hour_day_pivot = hour_day_pivot.reindex(day_order, fill_value=0)
                
                # Create heatmap
                fig = px.imshow(
                    hour_day_pivot,
                    labels=dict(x="Hour of Day", y="Day of Week", color="Session Count"),
                    title="Session Activity Heatmap by Hour and Day",
                    color_continuous_scale='Viridis',
                    x=all_hours,
                    y=day_order
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)

def main():
    fernet_key = encryptionKey.get_key()
    db = {
        'db_host': os.environ['POSTGRES_HOSTNAME'],
        'db_port': os.environ['POSTGRES_PORT'],
        'db_name': os.environ['POSTGRES_DATABASE'],
        'db_user': os.environ['POSTGRES_USER'],
        'db_password': os.environ['POSTGRES_PASSWORD']
    }
    
    # Using Streamlit cache for Database connection resource
    @st.cache_resource
    def get_conn_cursor(db):
        conn, cursor = pgDb.get_db_cursor(db=db)
        return conn, cursor

    conn, cursor = get_conn_cursor(db)
    
    # Check authentication
    if not st.session_state.get('authenticated'):
        st.warning("Please login to continue.")
        with st.sidebar:
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            if st.button("Login"):
                user_authenticated = userAuth.verify_user(cursor, username, password, fernet_key)
                if user_authenticated:
                    st.session_state.authenticated = True
                    st.session_state.username = username
                    st.success("Logged in successfully!")
                    st.rerun()
                else:
                    st.error("Invalid username or password")
        st.stop()
    else:
        with st.sidebar:
            st.success(f"Logged in as {st.session_state.username}")
            if st.button("Logout"):
                st.session_state.authenticated = False
                st.rerun()

    # Create a container for the filter widgets
    with st.container(border=True):
        st.subheader("Select Report Filters")
        
        # Date range selector
        col_date1, col_date2 = st.columns(2)
        with col_date1:
            start_date = st.date_input("Start Date", value=None)
        with col_date2:
            end_date = st.date_input("End Date", value=None)
        
        # Get data for selectors
        storage_df = stContainersDf.get_configured_storage(cursor=cursor)[['Name', 'StorageIP']]
        volumes_df = stContainersDf.get_all_volumes(cursor=cursor)[["Volume", "vserver", "Storage", "Protocol", "StorageType"]]
        servers_df = stContainersDf.get_servers(cursor=cursor)[['ServerIP']]
        session_users_df = stContainersDf.get_session_users(cursor=cursor)[['Username', 'Protocol']]
        col1, col2, col3 = st.columns([35, 25, 15])
        with col1:
            with st.container(border=True):                
                st.write("#### Select Volumes")
                volume_list = create_selectors(volumes_df, height=250)
                if not volume_list:
                    st.error("Please select at least one volume.")

        with col2:
            with st.container(border=True):
                # Create AgGrid selectors for filters
                st.write("#### Select Users")
                session_users_list = create_selectors(session_users_df, height=250)
                if not session_users_list:
                    st.error("Please select at least one storage system.")


        with col3:
            with st.container(border=True):
                st.write("#### Select Servers")
                server_list = create_selectors(servers_df, height=250)
                if not server_list:
                    st.error("Please select at least one server.")
                
        # Keep protocols as multiselect since it's a simple list
        selected_protocols = st.multiselect(
            "Select Protocols",
            options=['CIFS', 'NFS'],
            default=['CIFS', 'NFS']
        )
        if not selected_protocols:
            st.error("Please select at least one protocol.")
            selected_protocols = ['CIFS', 'NFS']

    # Convert date inputs to string format for SQL query if they exist
    start_date_str = start_date.strftime('%Y-%m-%d') if start_date else None
    end_date_str = end_date.strftime('%Y-%m-%d') if end_date else None
    
    # Create a container for the download button
    with st.container(border=True):
        st.subheader("Generate and Download Report")
        
        # Add some information about the selected filters
        if session_users_list and server_list and volume_list and selected_protocols:
            time_last, time_first, selected_count = stContainersDf.filtered_sessions_summary(
                cursor=cursor, 
                session_users_list=session_users_list, 
                server_list=server_list, 
                volume_list=volume_list, 
                protocol_list=selected_protocols
            )
            
            # st.write(f"Report will be generated by filtering **{selected_count}** records.")
            if not start_date and not end_date:
                st.write(f"Date range: From **{time_first}** to **{time_last}**")
            else:
                st.write(f"Date range: From **{start_date}** to **{end_date}**")
            st.write(f"Selected: **{len(session_users_list)}** storage systems, **{len(server_list)}** servers, **{len(volume_list)}** volumes, **{len(selected_protocols)}** protocols")
            
            # Create download button
            if st.button("Generate and Download Report", type="primary"):
                # Create a progress bar
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                # Show a spinner while processing
                with st.spinner("Processing your request..."):
                    # Update status
                    status_text.text("Querying database...")
                    
                    # Simulate progress for SQL query execution (since we can't track actual SQL progress)
                    for i in range(50):
                        # Update progress bar
                        progress_bar.progress(i * 0.01)
                        time.sleep(0.05)  # Small delay to simulate processing
                    
                    # Get the data
                    status_text.text("Retrieving data...")
                    sessions_df = stContainersDf.get_filtered_sessions(
                        cursor=cursor, 
                        session_users_list=session_users_list, 
                        server_list=server_list, 
                        volume_list=volume_list, 
                        protocol_list=selected_protocols, 
                        limit=selected_count,  # Get all records
                        offset=0,
                        start_date=start_date_str,
                        end_date=end_date_str
                    )
                    
                    # Update progress
                    for i in range(50, 90):
                        progress_bar.progress(i * 0.01)
                        time.sleep(0.02)  # Small delay to simulate processing
                    
                    # Prepare the CSV
                    status_text.text("Preparing CSV file...")
                    csv_data = sessions_df.to_csv(index=False).encode("utf-8")
                    
                    # Complete the progress bar
                    for i in range(90, 101):
                        progress_bar.progress(i * 0.01)
                        time.sleep(0.01)
                    
                    # Format the filename
                    try:
                        if not start_date:
                            start_date = time_first.date()
                        if not end_date:
                            end_date = time_last.date()
                    except AttributeError:
                        pass
                    filename = f'sessions-from-{start_date}-to-{end_date}.csv'
                    
                    # Show success message
                    status_text.text("Report ready for download!")
                    
                    # Provide download button for the generated CSV
                    st.download_button(
                        label="Download CSV Report",
                        data=csv_data,
                        file_name=filename,
                        mime="text/csv",
                    )
                    st.info(f"Report generated with **{len(sessions_df)}** records.")
                    
                    # Store the sessions data in session state for visualization
                    st.session_state.sessions_df = sessions_df
                    
                    # Show visual report in a new container
                    with st.container(border=True):
                        col1, col2 = st.columns([4, 1])
                        with col1:
                            st.subheader("Visual Report")
                            st.write("Below is an interactive visual report of your data. You can explore different aspects of the sessions data through the tabs.")

                        
                        # Create the interactive visual report
                        create_visual_report(sessions_df)
        else:
            st.info("Please select all required filters to generate a report.")

if __name__ == "__main__":
    main()